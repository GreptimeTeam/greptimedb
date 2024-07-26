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
use regex::Regex;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    update_one_one_output_keys, yaml_bool, yaml_field, yaml_fields, yaml_string, Processor,
    FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
};
use crate::etl::value::{Array, Map, Value};

pub(crate) const PROCESSOR_GSUB: &str = "gsub";

const REPLACEMENT_NAME: &str = "replacement";

/// A processor to replace all matches of a pattern in string by a replacement, only support string value, and array string value
#[derive(Debug, Default)]
pub struct GsubProcessor {
    fields: Fields,
    pattern: Option<Regex>,
    replacement: Option<String>,
    ignore_missing: bool,
}

impl GsubProcessor {
    fn with_fields(&mut self, mut fields: Fields) {
        update_one_one_output_keys(&mut fields);
        self.fields = fields;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn try_pattern(&mut self, pattern: &str) -> Result<(), String> {
        self.pattern = Some(Regex::new(pattern).map_err(|e| e.to_string())?);
        Ok(())
    }

    fn with_replacement(&mut self, replacement: impl Into<String>) {
        self.replacement = Some(replacement.into());
    }

    fn check(self) -> Result<Self, String> {
        if self.pattern.is_none() {
            return Err("pattern is required".to_string());
        }

        if self.replacement.is_none() {
            return Err("replacement is required".to_string());
        }

        Ok(self)
    }

    fn process_string_field(&self, val: &str, field: &Field) -> Result<Map, String> {
        let replacement = self.replacement.as_ref().unwrap();
        let new_val = self
            .pattern
            .as_ref()
            .unwrap()
            .replace_all(val, replacement)
            .to_string();
        let val = Value::String(new_val);

        let key = field.get_target_field();

        Ok(Map::one(key, val))
    }

    fn process_array_field(&self, arr: &Array, field: &Field) -> Result<Map, String> {
        let key = field.get_target_field();

        let re = self.pattern.as_ref().unwrap();
        let replacement = self.replacement.as_ref().unwrap();

        let mut result = Array::default();
        for val in arr.iter() {
            match val {
                Value::String(haystack) => {
                    let new_val = re.replace_all(haystack, replacement).to_string();
                    result.push(Value::String(new_val));
                }
                _ => {
                    return Err(format!(
                        "{} processor: expect string or array string, but got {val:?}",
                        self.kind()
                    ))
                }
            }
        }

        Ok(Map::one(key, Value::Array(result)))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for GsubProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = GsubProcessor::default();

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
                PATTERN_NAME => {
                    processor.try_pattern(&yaml_string(v, PATTERN_NAME)?)?;
                }
                REPLACEMENT_NAME => {
                    processor.with_replacement(yaml_string(v, REPLACEMENT_NAME)?);
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

impl crate::etl::processor::Processor for GsubProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_GSUB
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn fields(&self) -> &Fields {
        &self.fields
    }

    fn fields_mut(&mut self) -> &mut Fields {
        &mut self.fields
    }

    fn output_keys(&self) -> HashSet<String> {
        self.fields
            .iter()
            .map(|f| f.get_target_field().to_string())
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match val {
            Value::String(val) => self.process_string_field(val, field),
            Value::Array(arr) => self.process_array_field(arr, field),
            _ => Err(format!(
                "{} processor: expect string or array string, but got {val:?}",
                self.kind()
            )),
        }
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            let index = field.input_field.index;
            match val.get(index) {
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: missing field: {}",
                            self.kind(),
                            field.get_field_name()
                        ));
                    }
                }
                Some(v) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let mut map = self.exec_field(v, field)?;
                    field
                        .output_fields_index_mapping
                        .iter()
                        .for_each(|(k, output_index)| {
                            if let Some(v) = map.remove(k) {
                                val[*output_index] = v;
                            }
                        });
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::etl::field::Field;
    use crate::etl::processor::gsub::GsubProcessor;
    use crate::etl::processor::Processor;
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_string_value() {
        let mut processor = GsubProcessor::default();
        processor.try_pattern(r"\d+").unwrap();
        processor.with_replacement("xxx");

        let field = Field::new("message");
        let val = Value::String("123".to_string());
        let result = processor.exec_field(&val, &field).unwrap();

        assert_eq!(
            result,
            Map::one("message", Value::String("xxx".to_string()))
        );
    }

    #[test]
    fn test_array_string_value() {
        let mut processor = GsubProcessor::default();
        processor.try_pattern(r"\d+").unwrap();
        processor.with_replacement("xxx");

        let field = Field::new("message");
        let val = Value::Array(
            vec![
                Value::String("123".to_string()),
                Value::String("456".to_string()),
            ]
            .into(),
        );
        let result = processor.exec_field(&val, &field).unwrap();

        assert_eq!(
            result,
            Map::one(
                "message",
                Value::Array(
                    vec![
                        Value::String("xxx".to_string()),
                        Value::String("xxx".to_string())
                    ]
                    .into()
                )
            )
        );
    }
}
