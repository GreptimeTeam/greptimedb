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

use std::str::FromStr;
use std::sync::Arc;

use arrow::json::reader::infer_json_schema_from_iterator;
use arrow_schema::{ArrowError, Fields};
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray};
use datafusion_common::arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use datatypes::json::JsonStructureSettings;
use datatypes::scalars::ScalarVectorBuilder;
use datatypes::types::StructType;
use datatypes::vectors::StructVectorBuilder;
use derive_more::derive::Display;
use serde_json;

use crate::function::{Function, extract_args};

/// Parses the `String` into `JSONB`.
#[derive(Clone, Debug, Display)]
#[display("{}", self.name())]
pub(crate) struct ToJsonFunction {
    signature: Signature,
}

impl Default for ToJsonFunction {
    fn default() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
        }
    }
}

const NAME: &str = "to_json";

impl Function for ToJsonFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        // try not to provide exact struct fields
        Ok(DataType::Struct(Fields::empty()))
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::Utf8View)?;
        let json_strings = arg0.as_string_view();

        let size = json_strings.len();

        // Parse json_strings into serde_json::Value vector, mapping Arrow nulls to serde_json::Value::Null
        let mut json_values = Vec::with_capacity(size);
        for i in 0..size {
            if json_strings.is_null(i) {
                json_values.push(Ok(serde_json::Value::Null));
            } else {
                let serde_json_value =
                    serde_json::Value::from_str(json_strings.value(i)).map_err(|e| {
                        ArrowError::JsonError(format!("Failed to parse JSON string: {}", e))
                    });
                json_values.push(serde_json_value);
            }
        }

        // Convert JSON values to StructArray manually using builders
        let json_values: Vec<serde_json::Value> = json_values
            .into_iter()
            .map(|result| result.map_err(|e| DataFusionError::ArrowError(Box::new(e), None)))
            .collect::<datafusion_common::Result<Vec<_>>>()?;

        let inferred_schema = infer_json_schema_from_iterator(
            json_values
                .iter()
                .filter(|v| v.is_object())
                .map(|v| Ok(v.clone())),
        )?;

        let greptime_struct_type = StructType::try_from(&inferred_schema.fields).map_err(|e| {
            DataFusionError::Execution(format!(
                "Failed to convert arrow type to greptime internal {}",
                e
            ))
        })?;
        let json_settings = JsonStructureSettings::Structured(Some(greptime_struct_type.clone()));
        let greptime_values = json_values
            .into_iter()
            .map(|v| {
                json_settings.encode(v).map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Failed to encode serde_json to Greptime Value {}",
                        e
                    ))
                })
            })
            .collect::<datafusion_common::Result<Vec<_>>>()?;
        let mut struct_vector_builder = StructVectorBuilder::with_type_and_capacity(
            greptime_struct_type,
            greptime_values.len(),
        );
        for v in greptime_values.into_iter() {
            if let Some(struct_value) = v.as_struct().map_err(|e| {
                DataFusionError::Execution(format!("Failed to coerce value to struct: {}", e))
            })? {
                struct_vector_builder
                    .push_struct_value(struct_value)
                    .map_err(|e| {
                        DataFusionError::Execution(format!(
                            "Failed to convert to arrow array: {}",
                            e
                        ))
                    })?;
            } else {
                struct_vector_builder.push_null_struct_value();
            }
        }

        let struct_vector = struct_vector_builder.finish();
        let struct_array = struct_vector.take_array();

        Ok(ColumnarValue::Array(Arc::new(struct_array)))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{StringArray, StructArray};
    use arrow::datatypes::{DataType, Field};
    use datafusion_common::config::ConfigOptions;

    use super::*;

    #[test]
    fn test_to_json() {
        let udf = ToJsonFunction::default();

        // Create test JSON strings with consistent field types
        let json_strings = vec![
            r#"{"name": "Alice", "age": 30}"#,
            r#"{"name": "Bob", "age": 25}"#,
            r#"null"#,
            r#"{"name": "Charlie", "age": 35}"#,
        ];

        // Create StringArray from JSON strings
        let string_array = Arc::new(StringArray::from(json_strings));

        // Create ScalarFunctionArgs
        let func_args = ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(string_array)],
            arg_fields: vec![Arc::new(Field::new("arg_0", DataType::Utf8, false))],
            return_field: Arc::new(Field::new(
                "result",
                DataType::Struct(Fields::empty()),
                true,
            )),
            number_rows: 3,
            config_options: Arc::new(ConfigOptions::default()),
        };

        // Call invoke_with_args
        let result = udf.invoke_with_args(func_args).unwrap();

        // Verify the result is a StructArray
        match result {
            ColumnarValue::Array(array) => {
                // The array should be a StructArray
                assert_eq!(array.len(), 4);
                // We can verify it's a struct array by checking the data type
                assert!(matches!(array.data_type(), DataType::Struct(_)));
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                assert_eq!(struct_array.num_columns(), 2);
                assert!(struct_array.is_null(2));

                // Verify that we have the expected number of rows processed
                // (including null values)
            }
            _ => panic!("Expected ColumnarValue::Array"),
        }
    }
}
