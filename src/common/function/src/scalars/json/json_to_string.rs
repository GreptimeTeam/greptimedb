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
use datatypes::vectors::{MutableVector, StringVectorBuilder};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Converts the `JSONB` into `String`. It's useful for displaying JSONB content.
#[derive(Clone, Debug, Default)]
pub struct JsonToStringFunction;

const NAME: &str = "json_to_string";

impl Function for JsonToStringFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
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
        let datatype = jsons.data_type();
        let mut results = StringVectorBuilder::with_capacity(size);

        match datatype {
            // JSON data type uses binary vector
            ConcreteDataType::Binary(_) => {
                for i in 0..size {
                    let json = jsons.get_ref(i);

                    let json = json.as_binary();
                    let result = match json {
                        Ok(Some(json)) => match jsonb::from_slice(json) {
                            Ok(json) => {
                                let json = json.to_string();
                                Some(json)
                            }
                            Err(_) => {
                                return InvalidFuncArgsSnafu {
                                    err_msg: format!("Illegal json binary: {:?}", json),
                                }
                                .fail()
                            }
                        },
                        _ => None,
                    };

                    results.push(result.as_deref());
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

impl Display for JsonToStringFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_TO_STRING")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::BinaryVector;

    use super::*;

    #[test]
    fn test_json_to_string_function() {
        let json_to_string = JsonToStringFunction;

        assert_eq!("json_to_string", json_to_string.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            json_to_string
                .return_type(&[ConcreteDataType::json_datatype()])
                .unwrap()
        );

        assert!(matches!(json_to_string.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::json_datatype()]
        ));

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
        ];

        let jsonbs = json_strings
            .iter()
            .map(|s| {
                let value = jsonb::parse_value(s.as_bytes()).unwrap();
                value.to_vec()
            })
            .collect::<Vec<_>>();

        let json_vector = BinaryVector::from_vec(jsonbs);
        let args: Vec<VectorRef> = vec![Arc::new(json_vector)];
        let vector = json_to_string
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in json_strings.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_string().unwrap().unwrap();
            // remove whitespaces
            assert_eq!(gt.replace(" ", ""), result);
        }

        let invalid_jsonb = vec![b"invalid json"];
        let invalid_json_vector = BinaryVector::from_vec(invalid_jsonb);
        let args: Vec<VectorRef> = vec![Arc::new(invalid_json_vector)];
        let vector = json_to_string.eval(FunctionContext::default(), &args);
        assert!(vector.is_err());
    }
}
