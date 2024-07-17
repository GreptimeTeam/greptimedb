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

use ahash::HashSet;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    update_one_one_output_keys, yaml_bool, yaml_field, yaml_fields, yaml_string, Processor,
    FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, SEPARATOR_NAME,
};
use crate::etl::value::{Array, Map, Value};

pub(crate) const PROCESSOR_JOIN: &str = "join";

/// A processor to join each element of an array into a single string using a separator string between each element
#[derive(Debug, Default)]
pub struct JoinProcessor {
    fields: Fields,
    separator: Option<String>,
    ignore_missing: bool,
}

impl JoinProcessor {
    fn with_fields(&mut self, mut fields: Fields) {
        update_one_one_output_keys(&mut fields);
        self.fields = fields;
    }

    fn with_separator(&mut self, separator: impl Into<String>) {
        self.separator = Some(separator.into());
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn process_field(&self, arr: &Array, field: &Field) -> Result<Map, String> {
        let key = field.get_renamed_field();

        let sep = self.separator.as_ref().unwrap();
        let val = arr
            .iter()
            .map(|v| v.to_str_value())
            .collect::<Vec<String>>()
            .join(sep);

        Ok(Map::one(key, Value::String(val)))
    }

    fn check(self) -> Result<Self, String> {
        if self.separator.is_none() {
            return Err("separator is required".to_string());
        }

        Ok(self)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for JoinProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = JoinProcessor::default();

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got {k:?}"))?;
            match key {
                FIELD_NAME => {
                    processor.with_fields(Fields::one(yaml_field(v, FIELD_NAME)?));
                }
                FIELDS_NAME => {
                    processor.with_fields(yaml_fields(v, FIELDS_NAME)?);
                }
                SEPARATOR_NAME => {
                    processor.with_separator(yaml_string(v, SEPARATOR_NAME)?);
                }
                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }
                _ => {}
            }
        }

        processor.check()
    }
}

impl Processor for JoinProcessor {
    fn fields(&self) -> &Fields {
        &self.fields
    }

    fn kind(&self) -> &str {
        PROCESSOR_JOIN
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn fields_mut(&mut self) -> &mut Fields {
        &mut self.fields
    }

    fn output_keys(&self) -> HashSet<String> {
        self.fields
            .iter()
            .map(|f| f.get_renamed_field().to_string())
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match val {
            Value::Array(arr) => self.process_field(arr, field),
            _ => Err(format!(
                "{} processor: expect array value, but got {val:?}",
                self.kind()
            )),
        }
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            let index = field.input_field.index;
            match val.get(index) {
                Some(Value::Array(arr)) => {
                    let mut map = self.process_field(arr, field)?;
                    field.output_fields.iter().for_each(|(k, ouput_index)| {
                        if let Some(v) = map.remove(k) {
                            val.insert(*ouput_index, v);
                        }
                    });
                }
                Some(_) => {
                    return Err(format!(
                        "{} processor: expect array value, but got {val:?}",
                        self.kind()
                    ));
                }
                None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: expect array value, but got missing value",
                            self.kind()
                        ));
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::etl::field::Field;
    use crate::etl::processor::join::JoinProcessor;
    use crate::etl::processor::Processor;
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_join_processor() {
        let mut processor = JoinProcessor::default();
        processor.with_separator("-");

        let field = Field::new("test");
        let arr = Value::Array(
            vec![
                Value::String("a".to_string()),
                Value::String("b".to_string()),
            ]
            .into(),
        );
        let result = processor.exec_field(&arr, &field).unwrap();
        assert_eq!(result, Map::one("test", Value::String("a-b".to_string())));
    }
}
