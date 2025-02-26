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

use jsonpath_rust::JsonPath;
use snafu::{OptionExt, ResultExt};

use super::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, PipelineMap, Processor, FIELDS_NAME,
    FIELD_NAME, IGNORE_MISSING_NAME, JSON_PATH_NAME, JSON_PATH_RESULT_INDEX_NAME,
};
use crate::etl::error::{Error, Result};
use crate::etl::field::Fields;
use crate::etl_error::{
    JsonPathParseResultIndexSnafu, JsonPathParseSnafu, KeyMustBeStringSnafu,
    ProcessorMissingFieldSnafu,
};
use crate::Value;

pub(crate) const PROCESSOR_JSON_PATH: &str = "json_path";

impl TryFrom<&yaml_rust::yaml::Hash> for JsonPathProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;
        let mut json_path = None;
        let mut result_idex = None;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                FIELD_NAME => {
                    fields = Fields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fields(v, FIELDS_NAME)?;
                }

                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                JSON_PATH_RESULT_INDEX_NAME => {
                    result_idex = Some(v.as_i64().context(JsonPathParseResultIndexSnafu)? as usize);
                }

                JSON_PATH_NAME => {
                    let json_path_str = yaml_string(v, JSON_PATH_NAME)?;
                    json_path = Some(
                        JsonPath::try_from(json_path_str.as_str()).context(JsonPathParseSnafu)?,
                    );
                }

                _ => {}
            }
        }

        let processor = JsonPathProcessor {
            fields,
            json_path: json_path.context(ProcessorMissingFieldSnafu {
                processor: PROCESSOR_JSON_PATH,
                field: JSON_PATH_NAME,
            })?,
            ignore_missing,
            result_index: result_idex,
        };

        Ok(processor)
    }
}

#[derive(Debug)]
pub struct JsonPathProcessor {
    fields: Fields,
    json_path: JsonPath<Value>,
    ignore_missing: bool,
    result_index: Option<usize>,
}

impl Default for JsonPathProcessor {
    fn default() -> Self {
        JsonPathProcessor {
            fields: Fields::default(),
            json_path: JsonPath::try_from("$").unwrap(),
            ignore_missing: false,
            result_index: None,
        }
    }
}

impl JsonPathProcessor {
    fn process_field(&self, val: &Value) -> Result<Value> {
        let processed = self.json_path.find(val);
        match processed {
            Value::Array(arr) => {
                if let Some(index) = self.result_index {
                    Ok(arr.get(index).cloned().unwrap_or(Value::Null))
                } else {
                    Ok(Value::Array(arr))
                }
            }
            v => Ok(v),
        }
    }
}

impl Processor for JsonPathProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_JSON_PATH
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut PipelineMap) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_field();
            match val.get(index) {
                Some(v) => {
                    let processed = self.process_field(v)?;
                    let output_index = field.target_or_input_field();
                    val.insert(output_index.to_string(), processed);
                }
                None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::Map;

    #[test]
    fn test_json_path() {
        use super::*;
        use crate::Value;

        let json_path = JsonPath::try_from("$.hello").unwrap();
        let processor = JsonPathProcessor {
            json_path,
            result_index: Some(0),
            ..Default::default()
        };

        let result = processor
            .process_field(&Value::Map(Map::one(
                "hello",
                Value::String("world".to_string()),
            )))
            .unwrap();
        assert_eq!(result, Value::String("world".to_string()));
    }
}
