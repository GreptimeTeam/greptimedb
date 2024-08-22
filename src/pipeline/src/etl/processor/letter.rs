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
use crate::etl::field::{InputFieldInfo, NewFields, OneInputOneOutPutField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fileds, yaml_string, Processor, ProcessorBuilder,
    ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME, METHOD_NAME,
};
use crate::etl::value::Value;

pub(crate) const PROCESSOR_LETTER: &str = "letter";

#[derive(Debug, Default)]
enum Method {
    Upper,
    #[default]
    Lower,
    Capital,
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Method::Upper => write!(f, "upper"),
            Method::Lower => write!(f, "lower"),
            Method::Capital => write!(f, "capital"),
        }
    }
}

impl std::str::FromStr for Method {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "upper" => Ok(Method::Upper),
            "lower" => Ok(Method::Lower),
            "capital" => Ok(Method::Capital),
            _ => Err(format!("invalid method: {s}")),
        }
    }
}

#[derive(Debug, Default)]
pub struct LetterProcessorBuilder {
    fields: NewFields,
    method: Method,
    ignore_missing: bool,
}

impl ProcessorBuilder for LetterProcessorBuilder {
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

        let processor = LetterProcessor {
            real_fields,
            method: self.method,
            ignore_missing: self.ignore_missing,
        };
        ProcessorKind::Letter(processor)
    }
}

impl LetterProcessorBuilder {
    pub fn build(self, intermediate_keys: &[String]) -> LetterProcessor {
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

        LetterProcessor {
            real_fields,
            method: self.method,
            ignore_missing: self.ignore_missing,
        }
    }
}

/// only support string value
#[derive(Debug, Default)]
pub struct LetterProcessor {
    real_fields: Vec<OneInputOneOutPutField>,
    method: Method,
    ignore_missing: bool,
}

impl LetterProcessor {
    fn process_field(&self, val: &str) -> Result<Value, String> {
        let processed = match self.method {
            Method::Upper => val.to_uppercase(),
            Method::Lower => val.to_lowercase(),
            Method::Capital => capitalize(val),
        };
        let val = Value::String(processed);

        Ok(val)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for LetterProcessorBuilder {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut fields = NewFields::default();
        let mut method = Method::Lower;
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
                METHOD_NAME => {
                    method = yaml_string(v, METHOD_NAME)?.parse()?;
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        Ok(LetterProcessorBuilder {
            fields,
            method,
            ignore_missing,
        })
    }
}

impl Processor for LetterProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_LETTER
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.real_fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::String(s)) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let result = self.process_field(s)?;
                    let (_, output_index) = field.output();
                    val[*output_index] = result;
                    // field
                    //     .output_fields_index_mapping
                    //     .iter()
                    //     .for_each(|(k, output_index)| {
                    //         if let Some(v) = processed.remove(k) {
                    //             val[*output_index] = v;
                    //         }
                    //     });
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: missing field: {}",
                            self.kind(),
                            &field.input().name
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

fn capitalize(s: &str) -> String {
    let mut c = s.chars();
    match c.next() {
        None => String::new(),
        Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
    }
}

#[cfg(test)]
mod tests {
    use crate::etl::processor::letter::{LetterProcessor, Method};
    use crate::etl::value::Value;

    #[test]
    fn test_process() {
        {
            let processor = LetterProcessor {
                method: Method::Upper,
                ..Default::default()
            };
            let processed = processor.process_field("pipeline").unwrap();
            assert_eq!(Value::String("PIPELINE".into()), processed)
        }

        {
            let processor = LetterProcessor {
                method: Method::Lower,
                ..Default::default()
            };
            let processed = processor.process_field("Pipeline").unwrap();
            assert_eq!(Value::String("pipeline".into()), processed)
        }

        {
            let processor = LetterProcessor {
                method: Method::Capital,
                ..Default::default()
            };
            let processed = processor.process_field("pipeline").unwrap();
            assert_eq!(Value::String("Pipeline".into()), processed)
        }
    }
}
