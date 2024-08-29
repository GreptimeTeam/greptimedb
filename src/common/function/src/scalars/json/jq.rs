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

use crate::function::{Function, FunctionContext};

#[derive(Clone, Debug, Default)]
pub struct JqFunction;

const NAME: &str = "jq";

impl Function for JqFunction {
    fn name(&self) -> &str {
        "jq"
    }

    fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
        Ok(ConcreteDataType::string_datatype())
    }

    fn signature(&self) -> Signature {
        Signature::exact(
            vec![
                ConcreteDataType::string_datatype(),
                ConcreteDataType::json_datatype(),
            ],
            Volatility::Immutable,
        )
    }

    fn eval(&self, _func_ctx: FunctionContext, columns: &[VectorRef]) -> Result<VectorRef> {
        let jsons = &columns[1];
        let paths = &columns[0];

        let size = jsons.len();
        let datatype = jsons.data_type();
        let mut results = StringVectorBuilder::with_capacity(size);

        match datatype {
            ConcreteDataType::Binary(_) | ConcreteDataType::Json(_) => {
                for i in 0..size {
                    let json = jsons.get_ref(i);
                    let json = json.as_binary().unwrap();
                    let path = paths.get_ref(i);
                    let path = path.as_string().unwrap();
                    let result = match (json, path) {
                        (Some(json), Some(path)) => {
                            let json_path = match jsonb::jsonpath::parse_json_path(path.as_bytes())
                            {
                                Ok(json_path) => json_path,
                                Err(_) => {
                                    return InvalidFuncArgsSnafu {
                                        err_msg: format!("Invalid JSON path: {}", path),
                                    }
                                    .fail();
                                }
                            };
                            let mut sub_jsonb = Vec::new();
                            let mut sub_offsets = Vec::new();
                            match jsonb::get_by_path(
                                json,
                                json_path,
                                &mut sub_jsonb,
                                &mut sub_offsets,
                            ) {
                                Ok(_) => Some(jsonb::to_string(&sub_jsonb)),
                                Err(_) => None,
                            }
                        }
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

impl Display for JqFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JQ")
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
    fn test_jq_function() {
        let jq = JqFunction;

        assert_eq!("jq", jq.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            jq.return_type(&[
                ConcreteDataType::json_datatype(),
                ConcreteDataType::string_datatype()
            ])
            .unwrap()
        );

        assert!(matches!(jq.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::json_datatype(), ConcreteDataType::string_datatype()]
        ));

        let json_strings = [
            r#"{"a": {"b": 2}, "b": 2, "c": 3}"#,
            r#"{"a": 4, "b": {"c": 6}, "c": 6}"#,
            r#"{"a": 7, "b": 8, "c": {"a": 7}}"#,
        ];
        let paths = vec!["a", "b", "c"];
        let results = [r#"{"b":2}"#, r#"{"c":6}"#, r#"{"a":7}"#];

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
        let vector = jq.eval(FunctionContext::default(), &args).unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_string().unwrap().unwrap();
            assert_eq!(*gt, result);
        }
    }
}
