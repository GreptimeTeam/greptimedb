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

use regex::Regex;
use snafu::{OptionExt, ResultExt};

use crate::etl::error::{
    Error, GsubPatternRequiredSnafu, GsubReplacementRequiredSnafu, KeyMustBeStringSnafu,
    ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, RegexSnafu, Result,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME, PATTERN_NAME,
};
use crate::etl::value::Value;
use crate::etl::PipelineMap;

pub(crate) const PROCESSOR_GSUB: &str = "gsub";

const REPLACEMENT_NAME: &str = "replacement";

/// A processor to replace all matches of a pattern in string by a replacement, only support string value, and array string value
#[derive(Debug)]
pub struct GsubProcessor {
    fields: Fields,
    pattern: Regex,
    replacement: String,
    ignore_missing: bool,
}

impl GsubProcessor {
    fn process_string(&self, val: &str) -> Result<Value> {
        let new_val = self.pattern.replace_all(val, &self.replacement).to_string();
        let val = Value::String(new_val);

        Ok(val)
    }

    fn process(&self, val: &Value) -> Result<Value> {
        match val {
            Value::String(val) => self.process_string(val),
            _ => ProcessorExpectStringSnafu {
                processor: PROCESSOR_GSUB,
                v: val.clone(),
            }
            .fail(),
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for GsubProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;
        let mut pattern = None;
        let mut replacement = None;

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
                PATTERN_NAME => {
                    let pattern_str = yaml_string(v, PATTERN_NAME)?;
                    pattern = Some(Regex::new(&pattern_str).context(RegexSnafu {
                        pattern: pattern_str,
                    })?);
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

        Ok(GsubProcessor {
            fields,
            pattern: pattern.context(GsubPatternRequiredSnafu)?,
            replacement: replacement.context(GsubReplacementRequiredSnafu)?,
            ignore_missing,
        })
    }
}

impl crate::etl::processor::Processor for GsubProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_GSUB
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
    use crate::etl::processor::gsub::GsubProcessor;
    use crate::etl::value::Value;

    #[test]
    fn test_string_value() {
        let processor = GsubProcessor {
            fields: Fields::default(),
            pattern: regex::Regex::new(r"\d+").unwrap(),
            replacement: "xxx".to_string(),
            ignore_missing: false,
        };

        let val = Value::String("123".to_string());
        let result = processor.process(&val).unwrap();

        assert_eq!(result, Value::String("xxx".to_string()));
    }
}
