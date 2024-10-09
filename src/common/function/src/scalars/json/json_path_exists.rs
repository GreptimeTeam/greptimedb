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

/// Converts the `JSONB` into `String`. It's useful for displaying JSONB content.
#[derive(Clone, Debug, Default)]
pub struct JsonPathExistsFunction;

const NAME: &str = "json_path_exists";

impl Function for JsonPathExistsFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
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
        let datatype = jsons.data_type();
        let mut results = BooleanVectorBuilder::with_capacity(size);

        match datatype {
            // JSON data type uses binary vector
            ConcreteDataType::Binary(_) => {
                for i in 0..size {
                    let json = jsons.get_ref(i);
                    let path = paths.get_ref(i);

                    let json = json.as_binary();
                    let path = path.as_string();
                    let result = match (json, path) {
                        (Ok(Some(json)), Ok(Some(path))) => {
                            let json_path = jsonb::jsonpath::parse_json_path(path.as_bytes());
                            match json_path {
                                Ok(json_path) => jsonb::path_exists(json, json_path).ok(),
                                Err(_) => None,
                            }
                        }
                        _ => None,
                    };

                    results.push(result);
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
    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{BinaryVector, StringVector};

    use super::*;

    #[test]
    fn test_json_path_exists_function() {
        let json_path_exists = JsonPathExistsFunction;

        assert_eq!("json_path_exists", json_path_exists.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            json_path_exists
                .return_type(&[ConcreteDataType::json_datatype()])
                .unwrap()
        );

        assert!(matches!(json_path_exists.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::json_datatype()]
        ));

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
        ];
        let paths = vec!["$.a.b.c", "$.b", "$.c.a", ".d"];
        let results = [false, true, true, false];

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

        assert_eq!(4, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_boolean().unwrap().unwrap();
            assert_eq!(*gt, result);
        }
    }
}
