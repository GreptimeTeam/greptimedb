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
use std::sync::Arc;

use common_query::error::Result;
use datafusion_common::arrow::array::{Array, AsArray, BooleanBuilder};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};

use crate::function::{Function, extract_args};

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

                fn return_type(&self, _: &[DataType]) -> Result<DataType> {
                    Ok(DataType::Boolean)
                }

                fn signature(&self) -> Signature {
                    // TODO(LFC): Use a more clear type here instead of "Binary" for Json input, once we have a "Json" type.
                    Signature::uniform(
                        1,
                        vec![DataType::Binary, DataType::BinaryView],
                        Volatility::Immutable,
                    )
                }

                fn invoke_with_args(
                    &self,
                    args: ScalarFunctionArgs,
                ) -> datafusion_common::Result<ColumnarValue> {
                    let [arg0] = extract_args(self.name(), &args)?;

                    let arg0 = compute::cast(&arg0, &DataType::BinaryView)?;
                    let jsons = arg0.as_binary_view();
                    let size = jsons.len();
                    let mut builder = BooleanBuilder::with_capacity(size);

                    for i in 0..size {
                        let json = jsons.is_valid(i).then(|| jsons.value(i));
                        let result = match json {
                            Some(json) => {
                                Some(jsonb::[<is_ $json_type>](json))
                            }
                            _ => None,
                        };
                        builder.append_option(result);
                    }

                    Ok(ColumnarValue::Array(Arc::new(builder.finish())))
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

    use arrow_schema::Field;
    use datafusion_common::arrow::array::{AsArray, BinaryArray};

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
                func.return_type(&[DataType::Binary]).unwrap(),
                DataType::Boolean
            );
            assert_eq!(
                func.signature(),
                Signature::uniform(
                    1,
                    vec![DataType::Binary, DataType::BinaryView],
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
        let args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(
                BinaryArray::from_iter_values(jsonbs),
            ))],
            arg_fields: vec![],
            number_rows: 6,
            return_field: Arc::new(Field::new("", DataType::Boolean, false)),
            config_options: Arc::new(Default::default()),
        };

        for (func, expected_result) in json_is_functions.iter().zip(expected_results.iter()) {
            let result = func
                .invoke_with_args(args.clone())
                .and_then(|x| x.to_array(6))
                .unwrap();
            let vector = result.as_boolean();
            assert_eq!(vector.len(), json_strings.len());

            for (i, expected) in expected_result.iter().enumerate() {
                let result = vector.value(i);
                assert_eq!(result, *expected);
            }
        }
    }
}
