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

use common_query::error::{InvalidFuncArgsSnafu, Result, UnsupportedInputDataTypeSnafu};
use common_query::prelude::Signature;
use datafusion::logical_expr::Volatility;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::value::{ListValue, ListValueRef};
use datatypes::vectors::{ListVectorBuilder, MutableVector};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Get all the keys from the JSON object.
#[derive(Clone, Debug, Default)]
pub struct JsonObjectKeysFunction;

const NAME: &str = "json_object_keys";

impl Function for JsonObjectKeysFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::list_datatype(
            ConcreteDataType::string_datatype(),
        ))
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![ConcreteDataType::json_datatype()],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 1,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly one, have: {}",
                    columns.len()
                ),
            }
        );
        let jsons = &columns[0];

        let size = jsons.len();
        let mut results =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::string_datatype(), size);

        for i in 0..size {
            let json = jsons.get_ref(i);
            match json.data_type() {
                // JSON data type uses binary vector
                ConcreteDataType::Binary(_) => {
                    if let Ok(Some(json)) = json.as_binary()
                        && let Ok(json) = jsonb::from_slice(json)
                        && let Some(keys) = json.object_keys()
                        && let jsonb::Value::Array(keys) = keys
                    {
                        let result = ListValue::new(
                            keys.iter().map(|k| k.to_string().into()).collect(),
                            ConcreteDataType::string_datatype(),
                        );
                        results.push(Some(ListValueRef::Ref { val: &result }));
                    } else {
                        results.push(None)
                    }
                }

                _ => {
                    return UnsupportedInputDataTypeSnafu {
                        function: NAME,
                        datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                    }
                    .fail();
                }
            }
        }

        Ok(results.to_vector())
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

    use common_query::prelude::TypeSignature;
    use datatypes::value::{ListValueRef, Value};
    use datatypes::vectors::{BinaryVector, Vector};

    use super::*;

    #[test]
    fn test_json_object_keys_function() {
        let json_object_keys = JsonObjectKeysFunction;

        assert_eq!("json_object_keys", json_object_keys.name());
        assert_eq!(
            ConcreteDataType::list_datatype(ConcreteDataType::string_datatype(),),
            json_object_keys
                .return_type(&[ConcreteDataType::list_datatype(
                    ConcreteDataType::string_datatype(),
                )])
                .unwrap()
        );

        assert!(matches!(json_object_keys.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if valid_types == vec![ConcreteDataType::json_datatype()],
        ));

        let json_strings = [
            Some(r#"{"a": {"b": 2}, "b": 2, "c": 3}"#.to_string()),
            Some(r#"{"a": 1, "b": [1,2,3]}"#.to_string()),
            Some(r#"[1,2,3]"#.to_string()),
            Some(r#"null"#.to_string()),
        ];

        let results = [
            Some(Value::List(ListValue::new(
                vec![
                    Value::String(r#""a""#.into()),
                    Value::String(r#""b""#.into()),
                    Value::String(r#""c""#.into()),
                ],
                ConcreteDataType::string_datatype(),
            ))),
            Some(Value::List(ListValue::new(
                vec![
                    Value::String(r#""a""#.into()),
                    Value::String(r#""b""#.into()),
                ],
                ConcreteDataType::string_datatype(),
            ))),
            None,
            None,
        ];

        let jsonbs = json_strings
            .into_iter()
            .map(|s| s.map(|json| jsonb::parse_value(json.as_bytes()).unwrap().to_vec()))
            .collect::<Vec<_>>();

        let json_vector = BinaryVector::from(jsonbs);
        let args: Vec<VectorRef> = vec![Arc::new(json_vector)];
        let vector = json_object_keys
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(4, vector.len());

        let result = vector.get_ref(0);
        assert!(!result.is_null());
        let result_value = result.as_list().unwrap().unwrap();
        let ListValueRef::Indexed {
            vector: result_value,
            idx: _,
        } = result_value
        else {
            unreachable!()
        };

        for (i, expected) in results.iter().enumerate() {
            match expected {
                Some(expected_value) => {
                    assert_eq!(expected_value, &result_value.get(i));
                }
                None => {
                    assert!(result_value.is_null(i));
                }
            }
        }
    }
}
