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
use datafusion_common::arrow::array::{Array, AsArray, ListBuilder, StringBuilder};
use datafusion_common::arrow::datatypes::{DataType, Field};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

/// Returns the sorted top-level keys of a JSON object.
#[derive(Clone, Debug)]
pub(crate) struct JsonObjectKeysFunction {
    signature: Signature,
    aliases: Vec<String>,
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
            // align with databend
            aliases: vec!["object_keys".to_string()],
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
            DataType::Utf8,
            true,
        ))))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [jsons] = extract_args(self.name(), &args)?;
        let jsons = arrow::compute::cast(&jsons, &DataType::BinaryView)?;
        let jsons = jsons.as_binary_view();
        let mut builder = ListBuilder::with_capacity(StringBuilder::new(), jsons.len());

        for index in 0..jsons.len() {
            // SQL NULL produces a NULL list rather than an empty list.
            if jsons.is_null(index) {
                builder.append_null();
                continue;
            }

            // JSON inputs are stored as binary JSONB and malformed values are execution errors.
            let value = jsonb::parse_jsonb(jsons.value(index)).map_err(|error| {
                DataFusionError::Execution(format!("invalid json binary: {error}"))
            })?;

            // Valid non-object JSON values also produce a NULL list.
            let jsonb::Value::Object(object) = value else {
                builder.append_null();
                continue;
            };

            // JSONB object keys are already unique and stored in deterministic order.
            for key in object.keys() {
                builder.values().append_value(key);
            }
            builder.append(true);
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

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{
        Array, AsArray, BinaryArray, BinaryViewArray, LargeBinaryArray,
    };
    use datafusion_expr::TypeSignature;

    use super::*;

    fn jsonb(json: &str) -> Vec<u8> {
        jsonb::parse_value(json.as_bytes()).unwrap().to_vec()
    }

    fn invoke(input: Arc<dyn Array>) -> datafusion_common::Result<Arc<dyn Array>> {
        let number_rows = input.len();
        JsonObjectKeysFunction::default()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![ColumnarValue::Array(input)],
                arg_fields: vec![],
                number_rows,
                return_field: Arc::new(Field::new(
                    "x",
                    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                    true,
                )),
                config_options: Arc::new(Default::default()),
            })
            .and_then(|value| value.to_array(number_rows))
    }

    #[test]
    fn test_metadata_and_return_type() {
        let function = JsonObjectKeysFunction::default();

        assert_eq!(NAME, function.name());
        assert_eq!(["object_keys"], function.aliases());
        assert_eq!("JSON_OBJECT_KEYS", function.to_string());
        assert_eq!(
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            function.return_type(&[DataType::Binary]).unwrap()
        );
        assert_eq!(Volatility::Immutable, function.signature().volatility);
        assert_eq!(
            TypeSignature::Uniform(
                1,
                vec![
                    DataType::Binary,
                    DataType::LargeBinary,
                    DataType::BinaryView,
                    DataType::Null,
                ],
            ),
            function.signature().type_signature
        );
    }

    #[test]
    fn test_sorted_top_level_and_nested_object_keys() {
        let input = BinaryViewArray::from_iter_values([
            jsonb(r#"{"z": 1, "nested": {"inner": 2}, "a": 3}"#),
            jsonb(r#"{"outer": {"b": 1, "a": 2}, "leaf": true}"#),
        ]);
        let result = invoke(Arc::new(input)).unwrap();
        let lists = result.as_list::<i32>();

        assert_eq!([0, 3, 5], lists.value_offsets());
        assert_eq!(0, lists.null_count());
        assert_eq!(
            vec![
                Some("a"),
                Some("nested"),
                Some("z"),
                Some("leaf"),
                Some("outer")
            ],
            lists.values().as_string::<i32>().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_empty_object_is_valid_empty_list() {
        let result = invoke(Arc::new(LargeBinaryArray::from_iter_values([jsonb("{}")]))).unwrap();
        let lists = result.as_list::<i32>();

        assert!(lists.is_valid(0));
        assert_eq!([0, 0], lists.value_offsets());
        assert!(lists.values().as_string::<i32>().is_empty());
    }

    #[test]
    fn test_sql_null_and_non_object_values_are_null_lists() {
        let json_values = [
            None,
            Some(jsonb("null")),
            Some(jsonb("[1, 2]")),
            Some(jsonb(r#""text""#)),
            Some(jsonb("true")),
            Some(jsonb("42")),
            Some(jsonb("1.5")),
        ];
        let input = BinaryArray::from_iter(json_values.iter().map(|value| value.as_deref()));
        let result = invoke(Arc::new(input)).unwrap();
        let lists = result.as_list::<i32>();

        assert_eq!(json_values.len(), lists.len());
        assert_eq!(json_values.len(), lists.null_count());
        assert_eq!([0, 0, 0, 0, 0, 0, 0, 0], lists.value_offsets());
        assert!(lists.values().as_string::<i32>().is_empty());
    }

    #[test]
    fn test_mixed_row_validity_offsets_and_values() {
        let json_values = [
            Some(jsonb(r#"{"b": 2, "a": 1}"#)),
            None,
            Some(jsonb("{}")),
            Some(jsonb("[1]")),
            Some(jsonb(r#"{"c": 3}"#)),
        ];
        let input = BinaryArray::from_iter(json_values.iter().map(|value| value.as_deref()));
        let result = invoke(Arc::new(input)).unwrap();
        let lists = result.as_list::<i32>();

        assert_eq!([0, 2, 2, 2, 2, 3], lists.value_offsets());
        assert_eq!(
            [true, false, true, false, true],
            std::array::from_fn(|index| lists.is_valid(index))
        );
        assert_eq!(
            vec![Some("a"), Some("b"), Some("c")],
            lists.values().as_string::<i32>().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_unicode_special_keys_are_decoded_and_sorted() {
        let input = BinaryArray::from_iter_values([jsonb(
            r#"{"中": 1, "ä": 2, "a": 3, "\u0061 b": 4, "quote\"key": 5, "line\nkey": 6, "e\u0301": 7, "é": 8}"#,
        )]);
        let result = invoke(Arc::new(input)).unwrap();
        let lists = result.as_list::<i32>();

        assert_eq!(
            vec![
                Some("a"),
                Some("a b"),
                Some("e\u{301}"),
                Some("line\nkey"),
                Some("quote\"key"),
                Some("ä"),
                Some("é"),
                Some("中")
            ],
            lists.values().as_string::<i32>().iter().collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_malformed_binary_returns_execution_error() {
        let malformed_values = [
            b"invalid jsonb".as_slice(),
            b"{}".as_slice(),
            &[0x40, 0x00, 0x00, 0x01],
        ];

        for malformed in malformed_values {
            let error = invoke(Arc::new(BinaryArray::from_iter_values([malformed]))).unwrap_err();
            assert!(matches!(
                error,
                datafusion_common::DataFusionError::Execution(_)
            ));
            assert!(error.to_string().contains("invalid json binary"));
        }
    }
}
