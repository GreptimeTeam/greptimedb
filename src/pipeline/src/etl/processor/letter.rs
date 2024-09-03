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

use crate::etl::field::{Fields, OneInputOneOutputField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, ProcessorBuilder,
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
    fields: Fields,
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

    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind, String> {
        self.build(intermediate_keys).map(ProcessorKind::Letter)
    }
}

impl LetterProcessorBuilder {
    pub fn build(self, intermediate_keys: &[String]) -> Result<LetterProcessor, String> {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input = OneInputOneOutputField::build(
                "letter",
                intermediate_keys,
                field.input_field(),
                field.target_or_input_field(),
            )?;
            real_fields.push(input);
        }

        Ok(LetterProcessor {
            fields: real_fields,
            method: self.method,
            ignore_missing: self.ignore_missing,
        })
    }
}

/// only support string value
#[derive(Debug, Default)]
pub struct LetterProcessor {
    fields: Vec<OneInputOneOutputField>,
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
        let mut fields = Fields::default();
        let mut method = Method::Lower;
        let mut ignore_missing = false;

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .ok_or(format!("key must be a string, but got {k:?}"))?;
            match key {
                FIELD_NAME => {
                    fields = Fields::one(yaml_new_field(v, FIELD_NAME)?);
                }
                FIELDS_NAME => {
                    fields = yaml_new_fields(v, FIELDS_NAME)?;
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
        for field in self.fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::String(s)) => {
                    let result = self.process_field(s)?;
                    let (_, output_index) = field.output();
                    val[*output_index] = result;
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
