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

//! Removes ANSI color control codes from the input text.
//!
//! Similar to [`decolorize`](https://grafana.com/docs/loki/latest/query/log_queries/#removing-color-codes)
//! from Grafana Loki and [`strip_ansi_escape_codes`](https://vector.dev/docs/reference/vrl/functions/#strip_ansi_escape_codes)
//! from Vector VRL.

use once_cell::sync::Lazy;
use regex::Regex;
use snafu::OptionExt;

use crate::error::{
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::Value;
use crate::etl::PipelineMap;

pub(crate) const PROCESSOR_DECOLORIZE: &str = "decolorize";

static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\x1b\[[0-9;]*m").unwrap());

/// Remove ANSI color control codes from the input text.
#[derive(Debug, Default)]
pub struct DecolorizeProcessor {
    fields: Fields,
    ignore_missing: bool,
}

impl DecolorizeProcessor {
    fn process_string(&self, val: &str) -> Result<Value> {
        Ok(Value::String(RE.replace_all(val, "").into_owned()))
    }

    fn process(&self, val: &Value) -> Result<Value> {
        match val {
            Value::String(val) => self.process_string(val),
            _ => ProcessorExpectStringSnafu {
                processor: PROCESSOR_DECOLORIZE,
                v: val.clone(),
            }
            .fail(),
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for DecolorizeProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
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
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        Ok(DecolorizeProcessor {
            fields,
            ignore_missing,
        })
    }
}

impl crate::etl::processor::Processor for DecolorizeProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_DECOLORIZE
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut PipelineMap) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_field();
            match val.get(index) {
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
                    let result = self.process(v)?;
                    let output_index = field.target_or_input_field();
                    val.insert(output_index.to_string(), result);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decolorize_processor() {
        let processor = DecolorizeProcessor {
            fields: Fields::default(),
            ignore_missing: false,
        };

        let val = Value::String("\x1b[32mGreen\x1b[0m".to_string());
        let result = processor.process(&val).unwrap();
        assert_eq!(result, Value::String("Green".to_string()));

        let val = Value::String("Plain text".to_string());
        let result = processor.process(&val).unwrap();
        assert_eq!(result, Value::String("Plain text".to_string()));

        let val = Value::String("\x1b[46mfoo\x1b[0m bar".to_string());
        let result = processor.process(&val).unwrap();
        assert_eq!(result, Value::String("foo bar".to_string()));
    }
}
