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

use snafu::OptionExt;

use crate::error::{
    Error, KeyMustBeStringSnafu, LetterInvalidMethodSnafu, ProcessorExpectStringSnafu,
    ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME, METHOD_NAME,
};
use crate::etl::value::Value;
use crate::etl::PipelineMap;

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
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "upper" => Ok(Method::Upper),
            "lower" => Ok(Method::Lower),
            "capital" => Ok(Method::Capital),
            _ => LetterInvalidMethodSnafu { method: s }.fail(),
        }
    }
}

/// only support string value
#[derive(Debug, Default)]
pub struct LetterProcessor {
    fields: Fields,
    method: Method,
    ignore_missing: bool,
}

impl LetterProcessor {
    fn process_field(&self, val: &str) -> Result<Value> {
        let processed = match self.method {
            Method::Upper => val.to_uppercase(),
            Method::Lower => val.to_lowercase(),
            Method::Capital => capitalize(val),
        };
        let val = Value::String(processed);

        Ok(val)
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for LetterProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut method = Method::Lower;
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
                METHOD_NAME => {
                    method = yaml_string(v, METHOD_NAME)?.parse()?;
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        Ok(LetterProcessor {
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

    fn exec_mut(&self, val: &mut PipelineMap) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_field();
            match val.get(index) {
                Some(Value::String(s)) => {
                    let result = self.process_field(s)?;
                    let output_key = field.target_or_input_field();
                    val.insert(output_key.to_string(), result);
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    return ProcessorExpectStringSnafu {
                        processor: self.kind(),
                        v: v.clone(),
                    }
                    .fail();
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
