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
use datatypes::vectors::{BinaryVectorBuilder, MutableVector};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

/// Parses the `String` into `JSONB`.
#[derive(Clone, Debug, Default)]
pub struct ParseJsonFunction;

const NAME: &str = "parse_json";

impl Function for ParseJsonFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::json_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![ConcreteDataType::string_datatype()],
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
        let json_strings = &columns[0];

        let size = json_strings.len();
        let datatype = json_strings.data_type();
        let mut results = BinaryVectorBuilder::with_capacity(size);

        match datatype {
            ConcreteDataType::String(_) => {
                for i in 0..size {
                    let json_string = json_strings.get_ref(i);

                    let json_string = json_string.as_string();
                    let result = match json_string {
                        Ok(Some(json_string)) => match jsonb::parse_value(json_string.as_bytes()) {
                            Ok(json) => Some(json.to_vec()),
                            Err(_) => {
                                return InvalidFuncArgsSnafu {
                                    err_msg: format!(
                                        "Cannot convert the string to json, have: {}",
                                        json_string
                                    ),
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

impl Display for ParseJsonFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "PARSE_JSON")
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::prelude::TypeSignature;
    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::StringVector;

    use super::*;

    #[test]
    fn test_get_by_path_function() {
        let parse_json = ParseJsonFunction;

        assert_eq!("parse_json", parse_json.name());
        assert_eq!(
            ConcreteDataType::json_datatype(),
            parse_json
                .return_type(&[ConcreteDataType::json_datatype()])
                .unwrap()
        );

        assert!(matches!(parse_json.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::string_datatype()]
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

        let json_string_vector = StringVector::from_vec(json_strings.to_vec());
        let args: Vec<VectorRef> = vec![Arc::new(json_string_vector)];
        let vector = parse_json.eval(FunctionContext::default(), &args).unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in jsonbs.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_binary().unwrap().unwrap();
            assert_eq!(gt, result);
        }
    }
}
