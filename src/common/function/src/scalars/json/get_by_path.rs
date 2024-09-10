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
use datatypes::vectors::{
    BooleanVectorBuilder, Float64VectorBuilder, Int64VectorBuilder, MutableVector,
    StringVectorBuilder,
};
use snafu::ensure;

use crate::function::{Function, FunctionContext};

fn get_json_by_path(json: &[u8], path: &str) -> Option<Vec<u8>> {
    let json_path = jsonb::jsonpath::parse_json_path(path.as_bytes());
    match json_path {
        Ok(json_path) => {
            let mut sub_jsonb = Vec::new();
            let mut sub_offsets = Vec::new();
            match jsonb::get_by_path(json, json_path, &mut sub_jsonb, &mut sub_offsets) {
                Ok(_) => Some(sub_jsonb),
                Err(_) => None,
            }
        }
        _ => None,
    }
}

/// Get the value from the JSONB by the given path and return it as specified type.
/// If the path does not exist or the value is not the type specified, return `NULL`.
macro_rules! get_by_path {
    // e.g. name = GetByPathInt, type = Int64, rust_type = i64, doc = "Get the value from the JSONB by the given path and return it as an integer."
    ($name: ident, $type: ident, $rust_type: ident, $doc:expr) => {
        paste::paste! {
            #[doc = $doc]
            #[derive(Clone, Debug, Default)]
            pub struct $name;

            impl Function for $name {
                fn name(&self) -> &str {
                    stringify!([<$name:snake>])
                }

                fn return_type(&self, _input_types: &[ConcreteDataType]) -> Result<ConcreteDataType> {
                    Ok(ConcreteDataType::[<$type:snake _datatype>]())
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
                    let mut results = [<$type VectorBuilder>]::with_capacity(size);

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
                                        get_json_by_path(json, path)
                                            .and_then(|json| { jsonb::[<to_ $rust_type>](&json).ok() })
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
    };
}

get_by_path!(
    GetByPathInt,
    Int64,
    i64,
    "Get the value from the JSONB by the given path and return it as an integer."
);

get_by_path!(
    GetByPathFloat,
    Float64,
    f64,
    "Get the value from the JSONB by the given path and return it as a float."
);

get_by_path!(
    GetByPathBool,
    Boolean,
    bool,
    "Get the value from the JSONB by the given path and return it as a boolean."
);

/// Get the value from the JSONB by the given path and return it as a string.
#[derive(Clone, Debug, Default)]
pub struct GetByPathString;

impl Function for GetByPathString {
    fn name(&self) -> &str {
        "get_by_path_string"
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
        let mut results = StringVectorBuilder::with_capacity(size);

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
                            get_json_by_path(json, path).and_then(|json| jsonb::to_str(&json).ok())
                        }
                        _ => None,
                    };

                    results.push(result.as_deref());
                }
            }
            _ => {
                return UnsupportedInputDataTypeSnafu {
                    function: "get_by_path_string",
                    datatypes: columns.iter().map(|c| c.data_type()).collect::<Vec<_>>(),
                }
                .fail();
            }
        }

        Ok(results.to_vector())
    }
}

impl Display for GetByPathString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", "get_by_path_string".to_ascii_uppercase())
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
    fn test_get_by_path_int() {
        let get_by_path_int = GetByPathInt;

        assert_eq!("get_by_path_int", get_by_path_int.name());
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            get_by_path_int
                .return_type(&[
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::string_datatype()
                ])
                .unwrap()
        );

        assert!(matches!(get_by_path_int.signature(),
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
        let paths = vec!["$.a.b", "$.a", "$.c"];
        let results = [Some(2), Some(4), None];

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
        let vector = get_by_path_int
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_i64().unwrap();
            assert_eq!(*gt, result);
        }
    }

    #[test]
    fn test_get_by_path_float() {
        let get_by_path_float = GetByPathFloat;

        assert_eq!("get_by_path_float", get_by_path_float.name());
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            get_by_path_float
                .return_type(&[
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::string_datatype()
                ])
                .unwrap()
        );

        assert!(matches!(get_by_path_float.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::json_datatype(), ConcreteDataType::string_datatype()]
        ));

        let json_strings = [
            r#"{"a": {"b": 2.1}, "b": 2.2, "c": 3.3}"#,
            r#"{"a": 4.4, "b": {"c": 6.6}, "c": 6.6}"#,
            r#"{"a": 7.7, "b": 8.8, "c": {"a": 7.7}}"#,
        ];
        let paths = vec!["$.a.b", "$.a", "$.c"];
        let results = [Some(2.1), Some(4.4), None];

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
        let vector = get_by_path_float
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_f64().unwrap();
            assert_eq!(*gt, result);
        }
    }

    #[test]
    fn test_get_by_path_boolean() {
        let get_by_path_bool = GetByPathBool;

        assert_eq!("get_by_path_bool", get_by_path_bool.name());
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            get_by_path_bool
                .return_type(&[
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::string_datatype()
                ])
                .unwrap()
        );

        assert!(matches!(get_by_path_bool.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::json_datatype(), ConcreteDataType::string_datatype()]
        ));

        let json_strings = [
            r#"{"a": {"b": true}, "b": false, "c": true}"#,
            r#"{"a": false, "b": {"c": true}, "c": false}"#,
            r#"{"a": true, "b": false, "c": {"a": true}}"#,
        ];
        let paths = vec!["$.a.b", "$.a", "$.c"];
        let results = [Some(true), Some(false), None];

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
        let vector = get_by_path_bool
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_boolean().unwrap();
            assert_eq!(*gt, result);
        }
    }

    #[test]
    fn test_get_by_path_string() {
        let get_by_path_string = GetByPathString;

        assert_eq!("get_by_path_string", get_by_path_string.name());
        assert_eq!(
            ConcreteDataType::string_datatype(),
            get_by_path_string
                .return_type(&[
                    ConcreteDataType::json_datatype(),
                    ConcreteDataType::string_datatype()
                ])
                .unwrap()
        );

        assert!(matches!(get_by_path_string.signature(),
                         Signature {
                             type_signature: TypeSignature::Exact(valid_types),
                             volatility: Volatility::Immutable
                         } if  valid_types == vec![ConcreteDataType::json_datatype(), ConcreteDataType::string_datatype()]
        ));

        let json_strings = [
            r#"{"a": {"b": "a"}, "b": "b", "c": "c"}"#,
            r#"{"a": "d", "b": {"c": "e"}, "c": "f"}"#,
            r#"{"a": "g", "b": "h", "c": {"a": "g"}}"#,
        ];
        let paths = vec!["$.a.b", "$.a", ""];
        let results = [Some("a"), Some("d"), None];

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
        let vector = get_by_path_string
            .eval(FunctionContext::default(), &args)
            .unwrap();

        assert_eq!(3, vector.len());
        for (i, gt) in results.iter().enumerate() {
            let result = vector.get_ref(i);
            let result = result.as_string().unwrap();
            assert_eq!(*gt, result);
        }
    }
}
