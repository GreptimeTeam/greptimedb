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

// use super::{yaml_new_field, yaml_new_fileds, ProcessorBuilder, ProcessorKind};
use crate::etl::field::{Field, Fields, InputFieldInfo, NewFields, OneInputOneOutPutField};
use crate::etl::processor::{
    update_one_one_output_keys, yaml_bool, yaml_new_field, yaml_new_fileds, yaml_string, Processor,
    ProcessorBuilder, ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, SEPARATOR_NAME,
};
use crate::etl::value::{Array, Map, Value};

pub(crate) const PROCESSOR_JOIN: &str = "join";

#[derive(Debug, Default)]
pub struct JoinProcessorBuilder {
    fields: NewFields,
    separator: Option<String>,
    ignore_missing: bool,
}

impl ProcessorBuilder for JoinProcessorBuilder {
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
        ProcessorKind::Join(processor)
    }
}

impl JoinProcessorBuilder {
    fn check(self) -> Result<Self, String> {
        if self.separator.is_none() {
            return Err("separator is required".to_string());
        }

        Ok(self)
    }

    pub fn build(self, intermediate_keys: &[String]) -> JoinProcessor {
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

        JoinProcessor {
            real_fields,
            separator: self.separator,
            ignore_missing: self.ignore_missing,
        }
    }
}

/// A processor to join each element of an array into a single string using a separator string between each element
#[derive(Debug, Default)]
pub struct JoinProcessor {
    real_fields: Vec<OneInputOneOutPutField>,
    separator: Option<String>,
    ignore_missing: bool,
}

impl JoinProcessor {
    fn process(&self, arr: &Array) -> Result<Value, String> {
        let sep = self.separator.as_ref().unwrap();
        let val = arr
            .iter()
            .map(|v| v.to_str_value())
            .collect::<Vec<String>>()
            .join(sep);

        Ok(Value::String(val))
    }

    fn check(self) -> Result<Self, String> {
        if self.separator.is_none() {
            return Err("separator is required".to_string());
        }

        Ok(self)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for JoinProcessorBuilder {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut fields = NewFields::default();
        let mut separator = None;
        let mut ignore_missing = false;

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
                SEPARATOR_NAME => {
                    separator = Some(yaml_string(v, SEPARATOR_NAME)?);
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        let builder = JoinProcessorBuilder {
            fields,
            separator,
            ignore_missing,
        };
        builder.check()
    }
}

impl Processor for JoinProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_JOIN
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.real_fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::Array(arr)) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let result = self.process(arr)?;
                    let output_index = field.output_index();
                    val[output_index] = result;
                    // field
                    //     .output_fields_index_mapping
                    //     .iter()
                    //     .for_each(|(k, output_index)| {
                    //         if let Some(v) = map.remove(k) {
                    //             val[*output_index] = v;
                    //         }
                    //     });
                }
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
                    return Err(format!(
                        "{} processor: expect string value, but got {v:?}",
                        self.kind()
                    ));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use crate::etl::processor::join::JoinProcessor;
    use crate::etl::value::Value;

    #[test]
    fn test_join_processor() {
        let processor = JoinProcessor {
            separator: Some("-".to_string()),
            ..Default::default()
        };

        let arr = vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]
        .into();
        let result = processor.process(&arr).unwrap();
        assert_eq!(result, Value::String("a-b".to_string()));
    }
}
