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

/// Checks if the input is a JSON object of the given type.
macro_rules! json_is {
    ($name:ident, $json_type:ident, $doc:expr) => {
        paste::paste! {
            #[derive(Clone, Debug, Default)]
            pub struct $name;

            impl Function for $name {
                fn name(&self) -> &str {
                    stringify!([<$name:snake>])
                }

                fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
                    Ok(ConcreteDataType::boolean_datatype())
                }

                fn signature(&self) -> Signature {
                    Signature::exact(vec![ConcreteDataType::json_datatype()], Volatility::Immutable)
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
                    let mut results = BooleanVectorBuilder::with_capacity(size);

                    match datatype {
                        // JSON data type uses binary vector
                        ConcreteDataType::Binary(_) => {
                            for i in 0..size {
                                let json = jsons.get_ref(i);
                                let json = json.as_binary();
                                let result = match json {
                                    Ok(Some(json)) => {
                                        Some(jsonb::[<is_ $json_type>](json))
                                    }
                                    _ => None,
                                };
                                results.push(result);
                            }
                        }
                        _ => {
                            return UnsupportedInputDataTypeSnafu {
                                function: stringify!([<$name:snake>]),
                                datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                            }
                            .fail();
                        }
                    }

                    Ok(results.to_vector())
                }
            }

            impl Display for $name {
                fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
                    write!(f, "{}", stringify!([<$name:snake>]).to_ascii_uppercase())
                }
            }
        }
    }
}

json_is!(JsonIsNull, null, "Checks if the input JSONB is null");
json_is!(
    JsonIsBool,
    boolean,
    "Checks if the input JSONB is a boolean type JSON value"
);
json_is!(
    JsonIsInt,
    i64,
    "Checks if the input JSONB is a integer type JSON value"
);
json_is!(
    JsonIsFloat,
    number,
    "Checks if the input JSONB is a JSON float"
);
json_is!(
    JsonIsString,
    string,
    "Checks if the input JSONB is a JSON string"
);
json_is!(
    JsonIsArray,
    array,
    "Checks if the input JSONB is a JSON array"
);
json_is!(
    JsonIsObject,
    object,
    "Checks if the input JSONB is a JSON object"
);

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::BinaryVector;

    use super::*;

    #[test]
    fn test_json_is_functions() {
        let json_is_functions: [&dyn Function; 6] = [
            &JsonIsBool,
            &JsonIsInt,
            &JsonIsFloat,
            &JsonIsString,
            &JsonIsArray,
            &JsonIsObject,
        ];
        let expected_names = [
            "json_is_bool",
            "json_is_int",
            "json_is_float",
            "json_is_string",
            "json_is_array",
            "json_is_object",
        ];
        for (func, expected_name) in json_is_functions.iter().zip(expected_names.iter()) {
            assert_eq!(func.name(), *expected_name);
            assert_eq!(
                func.return_type(&[ConcreteDataType::json_datatype()])
                    .unwrap(),
                ConcreteDataType::boolean_datatype()
            );
            assert_eq!(
                func.signature(),
                Signature::exact(
                    vec![ConcreteDataType::json_datatype()],
                    Volatility::Immutable
                )
            );
        }

        let json_strings = [
            r#"true"#,
            r#"1"#,
            r#"1.0"#,
            r#""The pig fly through a castle, and has been attracted by the princess.""#,
            r#"[1, 2]"#,
            r#"{"a": 1}"#,
        ];
        let expected_results = [
            [true, false, false, false, false, false],
            [false, true, false, false, false, false],
            // Integers are also floats
            [false, true, true, false, false, false],
            [false, false, false, true, false, false],
            [false, false, false, false, true, false],
            [false, false, false, false, false, true],
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

        for (func, expected_result) in json_is_functions.iter().zip(expected_results.iter()) {
            let vector = func.eval(FunctionContext::default(), &args).unwrap();
            assert_eq!(vector.len(), json_strings.len());

            for (i, expected) in expected_result.iter().enumerate() {
                let result = vector.get_ref(i);
                let result = result.as_boolean().unwrap().unwrap();
                assert_eq!(result, *expected);
            }
        }
    }
}
