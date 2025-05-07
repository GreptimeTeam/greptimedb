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

use snafu::OptionExt as _;

use crate::error::{Error, KeyMustBeStringSnafu, ProcessorMissingFieldSnafu, Result};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME, KEY_NAME,
};
use crate::{PipelineMap, Processor, Value};

pub(crate) const PROCESSOR_SIMPLE_EXTRACT: &str = "simple_extract";

#[derive(Debug, Default)]
pub struct SimpleExtractProcessor {
    fields: Fields,
    /// simple keys to extract nested JSON field
    /// key `a.b` is saved as  ['a', 'b'], each key represents a level of the JSON tree
    key: Vec<String>,
    ignore_missing: bool,
}

impl TryFrom<&yaml_rust::yaml::Hash> for SimpleExtractProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;
        let mut keys = vec![];

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
                KEY_NAME => {
                    let key_str = yaml_string(v, KEY_NAME)?;
                    keys.extend(key_str.split(".").map(|s| s.to_string()));
                }
                _ => {}
            }
        }

        let processor = SimpleExtractProcessor {
            fields,
            key: keys,
            ignore_missing,
        };

        Ok(processor)
    }
}

impl SimpleExtractProcessor {
    fn process_field(&self, val: &Value) -> Result<Value> {
        let mut current = val;
        for key in self.key.iter() {
            let Value::Map(map) = current else {
                return Ok(Value::Null);
            };
            let Some(v) = map.get(key) else {
                return Ok(Value::Null);
            };
            current = v;
        }
        Ok(current.clone())
    }
}

impl Processor for SimpleExtractProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_SIMPLE_EXTRACT
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
    fn test_simple_extract() {
        use super::*;
        use crate::{Map, Value};

        let processor = SimpleExtractProcessor {
            key: vec!["hello".to_string()],
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
