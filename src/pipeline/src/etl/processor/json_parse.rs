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

use snafu::{OptionExt as _, ResultExt};

use crate::error::{
    Error, FieldMustBeTypeSnafu, JsonParseSnafu, KeyMustBeStringSnafu, ProcessorMissingFieldSnafu,
    ProcessorUnsupportedValueSnafu, Result,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::{json_to_map, PipelineMap, Processor, Value};

pub(crate) const PROCESSOR_JSON_PARSE: &str = "json_parse";

#[derive(Debug, Default)]
pub struct JsonParseProcessor {
    fields: Fields,
    ignore_missing: bool,
}

impl TryFrom<&yaml_rust::yaml::Hash> for JsonParseProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;

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
                _ => {}
            }
        }

        let processor = JsonParseProcessor {
            fields,
            ignore_missing,
        };

        Ok(processor)
    }
}

impl JsonParseProcessor {
    fn process_field(&self, val: &Value) -> Result<Value> {
        let Some(json_str) = val.as_str() else {
            return FieldMustBeTypeSnafu {
                field: val.to_str_type(),
                ty: "string",
            }
            .fail();
        };
        let parsed: serde_json::Value = serde_json::from_str(json_str).context(JsonParseSnafu)?;
        match parsed {
            serde_json::Value::Object(_) => Ok(Value::Map(json_to_map(parsed)?.into())),
            serde_json::Value::Array(arr) => Ok(Value::Array(arr.try_into()?)),
            _ => ProcessorUnsupportedValueSnafu {
                processor: self.kind(),
                val: val.to_str_type(),
            }
            .fail(),
        }
    }
}

impl Processor for JsonParseProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_JSON_PARSE
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

    #[test]
    fn test_json_parse() {
        use super::*;
        use crate::Value;

        let processor = JsonParseProcessor {
            ..Default::default()
        };

        let result = processor
            .process_field(&Value::String(r#"{"hello": "world"}"#.to_string()))
            .unwrap();

        let expected = Value::Map(crate::Map::one(
            "hello".to_string(),
            Value::String("world".to_string()),
        ));

        assert_eq!(result, expected);
    }
}
