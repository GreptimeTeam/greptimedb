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

        for i in 0..size {
            let json = jsons.get_ref(i);
            let path = paths.get_ref(i);

            match json.data_type() {
                ConcreteDataType::Binary(_) => {
                    // Since `json.data_type()` is `Binary`, we simply
                    // unwrap it without checking if it's `Null`.
                    let json = if let Ok(Some(bytes)) = json.as_binary() {
                        bytes
                    } else {
                        return InvalidFuncArgsSnafu {
                            err_msg: format!("Illegal json binary: {:?}", json),
                        }
                        .fail();
                    };

                    let result = if !path.is_null() {
                        // Since path is not null, we simply
                        // unwrap it without checking if it's `Null`.
                        let path = if let Ok(Some(str)) = path.as_string() {
                            if let Ok(json_path) = jsonb::jsonpath::parse_json_path(str.as_bytes())
                            {
                                json_path
                            } else {
                                return InvalidFuncArgsSnafu {
                                    err_msg: format!("Illegal json path {:?}", path),
                                }
                                .fail();
                            }
                        } else {
                            return InvalidFuncArgsSnafu {
                                err_msg: format!("Illegal json path: {:?}", path),
                            }
                            .fail();
                        };

                        jsonb::path_exists(json, path).ok()
                    } else {
                        None
                    };

                    results.push(result);
                }

                ConcreteDataType::Null(_) => results.push_null(),

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

impl Display for JsonPathExistsFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_PATH_EXISTS")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{BinaryVector, StringVector};

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
            Some(r#"{"a": {"b": 2}, "b": 2, "c": 3}"#),
            Some(r#"{"a": 4, "b": {"c": 6}, "c": 6}"#),
            Some(r#"{"a": 7, "b": 8, "c": {"a": 7}}"#),
            Some(r#"{"a": 7, "b": 8, "c": {"a": 7}}"#),
            Some(r#"[1, 2, 3]"#),
            Some(r#"null"#),
            Some(r#"{"a": 7, "b": 8, "c": {"a": 7}}"#),
            Some(r#"null"#),
            None,
            Some(r#"{"a": 7, "b": 8, "c": {"a": 7}}"#),
        ];
        let paths = vec![
            Some("$.a.b.c"),
            Some("$.b"),
            Some("$.c.a"),
            Some(".d"),
            Some("$[0]"),
            Some("$.a"),
            Some("null"),
            Some("null"),
            Some("$.a"),
            None,
        ];
        let expected = [
            Some(false),
            Some(true),
            Some(true),
            Some(false),
            Some(true),
            Some(false),
            Some(false),
            Some(false),
            None,
            None,
        ];

        let jsonbs = json_strings
            .iter()
            .map(|s| {
                if s.is_some() {
                    let value = jsonb::parse_value(s.unwrap().as_bytes()).unwrap();
                    Some(value.to_vec())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        let json_vector = BinaryVector::from(jsonbs);
        let path_vector = StringVector::from(paths);
        let args: Vec<VectorRef> = vec![Arc::new(json_vector), Arc::new(path_vector)];
        let vector = json_path_exists
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(10, vector.len());
        for (i, gt) in expected.iter().enumerate() {
            let result = vector.get_ref(i);
            if let Some(real) = gt {
                assert!(!result.is_null());
                let val = result.as_boolean().unwrap().unwrap();
                assert_eq!(val, *real);
            } else {
                assert!(result.is_null());
            }
        }
    }
}
