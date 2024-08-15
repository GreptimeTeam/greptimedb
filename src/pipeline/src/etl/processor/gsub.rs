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

use super::{yaml_new_field, yaml_new_fileds, ProcessorBuilder, ProcessorKind};
use crate::etl::field::{Field, Fields, InputFieldInfo, NewFields, OneInputOneOutPutField};
use crate::etl::processor::{
    update_one_one_output_keys, yaml_bool, yaml_field, yaml_fields, yaml_string, Processor,
    FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
};
use crate::etl::value::{Array, Map, Value};

pub(crate) const PROCESSOR_GSUB: &str = "gsub";

const REPLACEMENT_NAME: &str = "replacement";

#[derive(Debug, Default)]
pub struct GsubProcessorBuilder {
    fields: NewFields,
    pattern: Option<Regex>,
    replacement: Option<String>,
    ignore_missing: bool,
}

impl ProcessorBuilder for GsubProcessorBuilder {
    fn output_keys(&self) -> HashSet<&str> {
        todo!()
    }

    fn input_keys(&self) -> HashSet<&str> {
        todo!()
    }

    fn build(self, intermediate_keys: &[String]) -> ProcessorKind {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input_index = intermediate_keys
                .iter()
                .position(|k| *k == field.input_field())
                // TODO (qtang): handler error
                .unwrap();
            let input_field_info = InputFieldInfo::new(field.input_field(), input_index);
            let output_index = intermediate_keys
                .iter()
                .position(|k| *k == field.target_or_input_field())
                .unwrap();
            let input = OneInputOneOutPutField::new(
                input_field_info,
                (field.target_or_input_field().to_string(), output_index),
            );
            real_fields.push(input);
        }
        let processor = GsubProcessor {
            fields: Fields::one(Field::new("test".to_string())),
            real_fields,
            pattern: self.pattern,
            replacement: self.replacement,
            ignore_missing: self.ignore_missing,
        };
        ProcessorKind::Gsub(processor)
    }
}

impl GsubProcessorBuilder {
    fn check(self) -> Result<Self, String> {
        if self.pattern.is_none() {
            return Err("pattern is required".to_string());
        }

        if self.replacement.is_none() {
            return Err("replacement is required".to_string());
        }

        Ok(self)
    }

    fn build(self, intermediate_keys: &[String]) -> GsubProcessor {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input_index = intermediate_keys
                .iter()
                .position(|k| *k == field.input_field())
                // TODO (qtang): handler error
                .unwrap();
            let input_field_info = InputFieldInfo::new(field.input_field(), input_index);
            let output_index = intermediate_keys
                .iter()
                .position(|k| k == field.target_or_input_field())
                .unwrap();
            let input = OneInputOneOutPutField::new(
                input_field_info,
                (field.target_or_input_field().to_string(), output_index),
            );
            real_fields.push(input);
        }
        GsubProcessor {
            fields: Fields::one(Field::new("test".to_string())),
            real_fields,
            pattern: self.pattern,
            replacement: self.replacement,
            ignore_missing: self.ignore_missing,
        }
    }
}

/// A processor to replace all matches of a pattern in string by a replacement, only support string value, and array string value
#[derive(Debug, Default)]
pub struct GsubProcessor {
    fields: Fields,
    real_fields: Vec<OneInputOneOutPutField>,
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

impl TryFrom<&yaml_rust::yaml::Hash> for GsubProcessorBuilder {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut fields = NewFields::default();
        let mut ignore_missing = false;
        let mut pattern = None;
        let mut replacement = None;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got {k:?}"))?;
            match key {
                FIELD_NAME => {
                    fields = NewFields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fileds(v, FIELDS_NAME)?;
                }
                PATTERN_NAME => {
                    let pattern_str = yaml_string(v, PATTERN_NAME)?;
                    pattern = Some(Regex::new(&pattern_str).map_err(|e| e.to_string())?);
                }
                REPLACEMENT_NAME => {
                    let replacement_str = yaml_string(v, REPLACEMENT_NAME)?;
                    replacement = Some(replacement_str);
                }

                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }

                _ => {}
            }
        }

        let builder = GsubProcessorBuilder {
            fields,
            pattern,
            replacement,
            ignore_missing,
        };

        builder.check()
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

    fn output_keys(&self) -> HashSet<&str> {
        self.real_fields
            .iter()
            .map(|f| f.output().0.as_str())
            .collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.real_fields
            .iter()
            .map(|f| f.input().name.as_str())
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
