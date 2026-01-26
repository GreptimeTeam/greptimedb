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

use arrow::array::{Array, AsArray, BinaryBuilder};
use arrow::compute;
use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature};
use serde_json::Value as JsonValue;
use sql_json_path::JsonPath;

use crate::function::{extract_args, Function};
use crate::helper;

/// Query JSON data using the given JSON path.
#[derive(Clone, Debug)]
pub(crate) struct JsonPathQueryFunction {
    signature: Signature,
}

impl Default for JsonPathQueryFunction {
    fn default() -> Self {
        Self {
            signature: helper::one_of_sigs2(
                vec![DataType::Binary, DataType::BinaryView],
                vec![DataType::Utf8, DataType::Utf8View],
            ),
        }
    }
}

const NAME: &str = "json_path_query";

impl Function for JsonPathQueryFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;
        let arg0 = compute::cast(&arg0, &DataType::BinaryView)?;
        let jsons = arg0.as_binary_view();
        let arg1 = compute::cast(&arg1, &DataType::Utf8View)?;
        let paths = arg1.as_string_view();

        let size = jsons.len();
        let mut builder = BinaryBuilder::with_capacity(size, size * 32);

        for i in 0..size {
            let json = jsons.is_valid(i).then(|| jsons.value(i));
            let path = paths.is_valid(i).then(|| paths.value(i));

            let result = match (json, path) {
                (Some(json), Some(path)) => {
                    if !jsonb::is_null(json) {
                        let jsonb_value = jsonb::from_slice(json).map_err(|e| {
                            DataFusionError::Execution(format!("invalid jsonb binary: {e}"))
                        })?;
                        let json_value: JsonValue = jsonb_value.into();
                        let json_path = JsonPath::new(path).map_err(|e| {
                            DataFusionError::Execution(format!("invalid json path '{path}': {e}"))
                        })?;
                        let nodes = json_path.query(&json_value).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "failed to evaluate json path '{path}': {e}"
                            ))
                        })?;
                        let node_values: Vec<JsonValue> = nodes
                            .iter()
                            .map(|n| {
                                serde_json::from_str::<JsonValue>(&n.to_string())
                                    .unwrap_or(JsonValue::Null)
                            })
                            .collect();
                        if !node_values.is_empty() {
                            let result_json = JsonValue::Array(node_values);
                            let result_jsonb: jsonb::Value = result_json.into();
                            Some(result_jsonb.to_vec())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            };
            builder.append_option(result.as_deref());
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

impl Display for JsonPathQueryFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "JSON_PATH_QUERY")
    }
}
