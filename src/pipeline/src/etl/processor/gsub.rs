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

// use super::{yaml_new_field, yaml_new_fileds, ProcessorBuilder, ProcessorKind};
use crate::etl::field::{Fields, InputFieldInfo, NewFields, OneInputOneOutPutField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fileds, yaml_string, Processor, ProcessorBuilder,
    ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, PATTERN_NAME,
};
use crate::etl::value::{Array, Value};

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
        self.fields
            .iter()
            .map(|f| f.target_or_input_field())
            .collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.fields.iter().map(|f| f.input_field()).collect()
    }

    fn build(self, intermediate_keys: &[String]) -> ProcessorKind {
        let processor = Self::build(self, intermediate_keys);
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
    real_fields: Vec<OneInputOneOutPutField>,
    pattern: Option<Regex>,
    replacement: Option<String>,
    ignore_missing: bool,
}

impl GsubProcessor {
    fn check(self) -> Result<Self, String> {
        if self.pattern.is_none() {
            return Err("pattern is required".to_string());
        }

        if self.replacement.is_none() {
            return Err("replacement is required".to_string());
        }

        Ok(self)
    }

    fn process_string(&self, val: &str) -> Result<Value, String> {
        let replacement = self.replacement.as_ref().unwrap();
        let new_val = self
            .pattern
            .as_ref()
            .unwrap()
            .replace_all(val, replacement)
            .to_string();
        let val = Value::String(new_val);

        Ok(val)
    }

    fn process_array(&self, arr: &Array) -> Result<Value, String> {
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

        Ok(Value::Array(result))
    }

    fn process(&self, val: &Value) -> Result<Value, String> {
        match val {
            Value::String(val) => self.process_string(val),
            Value::Array(arr) => self.process_array(arr),
            _ => Err(format!(
                "{} processor: expect string or array string, but got {val:?}",
                self.kind()
            )),
        }
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

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.real_fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: missing field: {}",
                            self.kind(),
                            field.input_name()
                        ));
                    }
                }
                Some(v) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let result = self.process(v)?;
                    let output_index = field.output_index();
                    val[output_index] = result;
                    // let mut map = self.exec_field(v, field)?;
                    // field
                    //     .output_fields_index_mapping
                    //     .iter()
                    //     .for_each(|(k, output_index)| {
                    //         if let Some(v) = map.remove(k) {
                    //             val[*output_index] = v;
                    //         }
                    //     });
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
        // let mut processor = GsubProcessor::default();
        // processor.try_pattern(r"\d+").unwrap();
        // processor.with_replacement("xxx");

        // let field = Field::new("message");
        // let val = Value::String("123".to_string());
        // let result = processor.exec_field(&val, &field).unwrap();

        // assert_eq!(
        //     result,
        //     Map::one("message", Value::String("xxx".to_string()))
        // );
    }

    // no longer support
    // #[test]
    // fn test_array_string_value() {
    //     let mut processor = GsubProcessor::default();
    //     processor.try_pattern(r"\d+").unwrap();
    //     processor.with_replacement("xxx");

    //     let field = Field::new("message");
    //     let val = Value::Array(
    //         vec![
    //             Value::String("123".to_string()),
    //             Value::String("456".to_string()),
    //         ]
    //         .into(),
    //     );
    //     let result = processor.exec_field(&val, &field).unwrap();

    //     assert_eq!(
    //         result,
    //         Map::one(
    //             "message",
    //             Value::Array(
    //                 vec![
    //                     Value::String("xxx".to_string()),
    //                     Value::String("xxx".to_string())
    //                 ]
    //                 .into()
    //             )
    //         )
    //     );
    // }
}
