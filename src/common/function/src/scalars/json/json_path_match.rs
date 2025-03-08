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
use datatypes::vectors::{BooleanVectorBuilder, MutableVector};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Check if the given JSON data match the given JSON path's predicate.
#[derive(Clone, Debug, Default)]
pub struct JsonPathMatchFunction;

const NAME: &str = "json_path_match";

impl Function for JsonPathMatchFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::boolean_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![
                ConcreteDataType::json_datatype(),
                ConcreteDataType::string_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: &FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
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
                // JSON data type uses binary vector
                ConcreteDataType::Binary(_) => {
                    let json = json.as_binary();
                    let path = path.as_string();
                    let result = match (json, path) {
                        (Ok(Some(json)), Ok(Some(path))) => {
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

                    results.push(result);
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

impl Display for JsonPathMatchFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_PATH_MATCH")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::vectors::{BinaryVector, StringVector};

    use super::*;

    #[test]
    fn test_json_path_match_function() {
        let json_path_match = JsonPathMatchFunction;

        assert_eq!("json_path_match", json_path_match.name());
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            json_path_match
                .return_type(&[ConcreteDataType::json_datatype()])
                .unwrap()
        );

        assert!(matches!(json_path_match.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if valid_types == vec![ConcreteDataType::json_datatype(), ConcreteDataType::string_datatype()],
        ));

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

        let json_vector = BinaryVector::from(jsonbs);
        let path_vector = StringVector::from(paths);
        let args: Vec<VectorRef> = vec![Arc::new(json_vector), Arc::new(path_vector)];
        let vector = json_path_match
            .eval(&FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(7, vector.len());
        for (i, expected) in results.iter().enumerate() {
            let result = vector.get_ref(i);

            match expected {
                Some(expected_value) => {
                    assert!(!result.is_null());
                    let result_value = result.as_boolean().unwrap().unwrap();
                    assert_eq!(*expected_value, result_value);
                }
                None => {
                    assert!(result.is_null());
                }
            }
        }
    }
}
