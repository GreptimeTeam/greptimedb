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
use common_query::prelude::{Signature, TypeSignature};
use datafusion::logical_expr::Volatility;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::VectorRef;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::vectors::{BooleanVectorBuilder, MutableVector};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Check if the given JSON data contains the given JSON path.
#[derive(Clone, Debug, Default)]
pub struct JsonPathExistsFunction;

const NAME: &str = "json_path_exists";

impl Function for JsonPathExistsFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::one_of(
            vec![
                TypeSignature::Exact(vec![
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::string_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::null_datatype(),
                    ConcreteDataType::string_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::null_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::null_datatype(),
                    ConcreteDataType::null_datatype(),
                ]),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        ensure!(
            columns.len() == 2,
            InvalidFuncArgsSnafu {
                err_msg: format!(
                    "The length of the args is not correct, expect exactly two, have: {}",
                    columns.len()
                ),
            }
        );
        let jsons = &columns[0];
        let paths = &columns[1];

        let size = jsons.len();
        let mut results = BooleanVectorBuilder::with_capacity(size);

        match (jsons.data_type(), paths.data_type()) {
            (ConcreteDataType::Binary(_), ConcreteDataType::String(_)) => {
                for i in 0..size {
                    let result = match (jsons.get_ref(i).as_binary(), paths.get_ref(i).as_string())
                    {
                        (Ok(Some(json)), Ok(Some(path))) => {
                            // Get `JsonPath`.
                            let json_path = match jsonb::jsonpath::parse_json_path(path.as_bytes())
                            {
                                Ok(json_path) => json_path,
                                Err(_) => {
                                    return InvalidFuncArgsSnafu {
                                        err_msg: format!("Illegal json path: {:?}", path),
                                    }
                                    .fail();
                                }
                            };
                            jsonb::path_exists(json, json_path).ok()
                        }
                        _ => None,
                    };

                    results.push(result);
                }
            }

            // Any null args existence causes the result to be NULL.
            (ConcreteDataType::Null(_), ConcreteDataType::String(_)) => results.push_nulls(size),
            (ConcreteDataType::Binary(_), ConcreteDataType::Null(_)) => results.push_nulls(size),
            (ConcreteDataType::Null(_), ConcreteDataType::Null(_)) => results.push_nulls(size),

            _ => {
                return UnsupportedInputDataTypeSnafu {
                    function: NAME,
                    datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            }
        }

        Ok(results.to_vector())
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

    use common_query::prelude::TypeSignature;
    use datatypes::prelude::ScalarVector;
    use datatypes::vectors::{BinaryVector, NullVector, StringVector};

    use super::*;

    #[test]
    fn test_json_path_exists_function() {
        let json_path_exists = JsonPathExistsFunction;

        assert_eq!("json_path_exists", json_path_exists.name());
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            json_path_exists
                .return_type(&[ConcreteDataType::json_datatype()])
                .unwrap()
        );

        assert!(matches!(json_path_exists.signature(),
                         Signature {
                             type_signature: TypeSignature::OneOf(valid_types),
                             volatility: Volatility::Immutable
                         } if valid_types ==
            vec![
                TypeSignature::Exact(vec![
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::string_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::null_datatype(),
                    ConcreteDataType::string_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::null_datatype(),
                ]),
                TypeSignature::Exact(vec![
                    ConcreteDataType::null_datatype(),
                    ConcreteDataType::null_datatype(),
                ]),
            ],
        ));

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

        let json_vector = BinaryVector::from_vec(jsonbs);
        let path_vector = StringVector::from_vec(paths);
        let args: Vec<VectorRef> = vec![Arc::new(json_vector), Arc::new(path_vector)];
        let vector = json_path_exists
            .eval(FunctionContext::default(), &args)
            .unwrap();

        // Test for non-nulls.
        assert_eq!(8, vector.len());
        for (i, real) in expected.iter().enumerate() {
            let result = vector.get_ref(i);
            assert!(!result.is_null());
            let val = result.as_boolean().unwrap().unwrap();
            assert_eq!(val, *real);
        }

        // Test for nulls.
        let json_bytes = jsonb::parse_value("{}".as_bytes()).unwrap().to_vec();
        let json = BinaryVector::from_vec(vec![json_bytes]);
        let null_json = NullVector::new(1);

        let path = StringVector::from_vec(vec!["$.a"]);
        let null_path = NullVector::new(1);

        let args: Vec<VectorRef> = vec![Arc::new(null_json), Arc::new(path)];
        let result1 = json_path_exists
            .eval(FunctionContext::default(), &args)
            .unwrap();
        let args: Vec<VectorRef> = vec![Arc::new(json), Arc::new(null_path)];
        let result2 = json_path_exists
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(result1.len(), 1);
        assert!(result1.get_ref(0).is_null());
        assert_eq!(result2.len(), 1);
        assert!(result2.get_ref(0).is_null());
    }
}
