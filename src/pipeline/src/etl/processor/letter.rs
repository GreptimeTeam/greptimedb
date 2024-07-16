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

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, yaml_string, Processor, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME, METHOD_NAME,
};
use crate::etl::value::{Map, Value};
use ahash::HashSet;

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

/// only support string value
#[derive(Debug, Default)]
pub struct LetterProcessor {
    fields: Fields,
    method: Method,
    ignore_missing: bool,
}

impl LetterProcessor {
    fn with_fields(&mut self, fields: Fields) {
        self.fields = fields;
    }

    fn with_method(&mut self, method: Method) {
        self.method = method;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn process_field(&self, val: &str, field: &Field) -> Result<Map, String> {
        let processed = match self.method {
            Method::Upper => val.to_uppercase(),
            Method::Lower => val.to_lowercase(),
            Method::Capital => capitalize(val),
        };
        let val = Value::String(processed);

        let key = match field.target_field {
            Some(ref target_field) => target_field,
            None => field.get_field(),
        };

        Ok(Map::one(key, val))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for LetterProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = LetterProcessor::default();

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
                METHOD_NAME => {
                    let method = yaml_string(v, METHOD_NAME)?;
                    processor.with_method(method.parse()?);
                }
                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }
                _ => {}
            }
        }

        Ok(processor)
    }
}

impl Processor for LetterProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_LETTER
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
            Value::String(val) => self.process_field(val, field),
            _ => Err(format!(
                "{} processor: expect string value, but got {val:?}",
                self.kind()
            )),
        }
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
    use crate::etl::field::Fields;
    use crate::etl::processor::letter::{LetterProcessor, Method};
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_process() {
        let field = "letter";
        let ff: crate::etl::processor::Field = field.parse().unwrap();
        let mut processor = LetterProcessor::default();
        processor.with_fields(Fields::one(ff.clone()));

        {
            processor.with_method(Method::Upper);
            let processed = processor.process_field("pipeline", &ff).unwrap();
            assert_eq!(Map::one(field, Value::String("PIPELINE".into())), processed)
        }

        {
            processor.with_method(Method::Lower);
            let processed = processor.process_field("Pipeline", &ff).unwrap();
            assert_eq!(Map::one(field, Value::String("pipeline".into())), processed)
        }

        {
            processor.with_method(Method::Capital);
            let processed = processor.process_field("pipeline", &ff).unwrap();
            assert_eq!(Map::one(field, Value::String("Pipeline".into())), processed)
        }
    }
}
