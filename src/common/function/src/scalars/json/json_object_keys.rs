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

use arrow::array::{Array, AsArray, ListBuilder, StringViewBuilder};
use arrow::compute;
use arrow::datatypes::{DataType, Field};
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

/// Returns the keys of the outermost JSON object as a list of strings.
#[derive(Clone, Debug)]
pub(crate) struct JsonObjectKeysFunction {
    signature: Signature,
}

impl Default for JsonObjectKeysFunction {
    fn default() -> Self {
        Self {
            signature: Signature::uniform(
                1,
                vec![
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                    DataType::Null,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

const NAME: &str = "json_object_keys";

impl Function for JsonObjectKeysFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::List(Arc::new(Field::new(
            "item",
            DataType::Utf8View,
            true,
        ))))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [jsons] = extract_args(self.name(), &args)?;
        let jsons = compute::cast(&jsons, &DataType::BinaryView)?;
        let jsons = jsons.as_binary_view();

        let size = jsons.len();
        let mut builder = ListBuilder::with_capacity(StringViewBuilder::new(), size);

        for i in 0..size {
            let Some(json) = jsons.is_valid(i).then(|| jsons.value(i)) else {
                builder.append_null();
                continue;
            };

            match jsonb::from_slice(json) {
                Ok(jsonb::Value::Object(object)) => {
                    for key in object.keys() {
                        builder.values().append_value(key);
                    }
                    builder.append(true);
                }
                Ok(_) => builder.append_null(),
                Err(e) => {
                    return Err(DataFusionError::Execution(format!(
                        "invalid json binary: {e}"
                    )));
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonObjectKeysFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_OBJECT_KEYS")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryArray, NullArray};
    use arrow_schema::Field;

    use super::*;

    #[test]
    fn test_json_object_keys_function() {
        let json_object_keys = JsonObjectKeysFunction::default();
        let return_type = DataType::List(Arc::new(Field::new("item", DataType::Utf8View, true)));

        assert_eq!("json_object_keys", json_object_keys.name());
        assert_eq!(
            return_type,
            json_object_keys.return_type(&[DataType::Binary]).unwrap()
        );

        let json_strings = [
            Some(r#"{"b": 2, "a": 1}"#),
            Some("{}"),
            Some(r#"{"outer": {"inner": 1}, "value": 2}"#),
            Some("[1, 2]"),
            Some("42"),
            Some("null"),
            None,
        ];

        let results = [
            Some(vec!["a", "b"]),
            Some(vec![]),
            Some(vec!["outer", "value"]),
            None,
            None,
            None,
            None,
        ];

        let jsonbs = json_strings
            .into_iter()
            .map(|s| s.map(|json| jsonb::parse_value(json.as_bytes()).unwrap().to_vec()))
            .collect::<Vec<_>>();

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(BinaryArray::from_iter(
                jsonbs,
            )))],
            arg_fields: vec![],
            number_rows: 7,
            return_field: Arc::new(Field::new("x", return_type.clone(), true)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_object_keys
            .invoke_with_args(args)
            .and_then(|x| x.to_array(7))
            .unwrap();
        let vector = result.as_list::<i32>();

        assert_eq!(7, vector.len());
        for (i, expected) in results.iter().enumerate() {
            match expected {
                Some(expected) => {
                    let values = vector.value(i);
                    let values = values.as_string_view();
                    let keys = values.iter().flatten().collect::<Vec<_>>();
                    assert_eq!(expected, &keys);
                }
                None => assert!(vector.is_null(i)),
            }
        }

        let invalid_jsonb = vec![b"invalid json"];
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                BinaryArray::from_iter_values(invalid_jsonb),
            ))],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", return_type.clone(), true)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_object_keys.invoke_with_args(args);
        assert!(result.is_err());

        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(NullArray::new(1)))],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", return_type, true)),
            config_options: Arc::new(Default::default()),
        };
        let result = json_object_keys
            .invoke_with_args(args)
            .and_then(|x| x.to_array(1))
            .unwrap();
        let vector = result.as_list::<i32>();
        assert!(vector.is_null(0));
    }
}
