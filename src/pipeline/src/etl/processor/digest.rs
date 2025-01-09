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

//! Digest the input string by removing certain patterns.
//!
//! This processor can helps to extract useful information from a string by removing certain patterns,
//! which is often a variable from the log message. Digested fields are stored in a new field with the
//! `_digest` suffix. And can be used for further processing or analysis like template occurrences count
//! or similarity analysis.

use ahash::HashSet;
use regex::Regex;
use snafu::OptionExt;

use crate::etl::error::{
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::{Fields, OneInputOneOutputField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, ProcessorBuilder, ProcessorKind, FIELDS_NAME,
    FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::Value;
use crate::etl_error::PresetPatternInvalidSnafu;

pub(crate) const PROCESSOR_DIGEST: &str = "digest";

const PRESETS_PATTERNS_NAME: &str = "presets";
const REGEX_PATTERNS_NAME: &str = "regex";

enum PresetPattern {
    Numbers,
    Quoted,
    Bracketed,
    Uuid,
    Ip,
}

impl std::fmt::Display for PresetPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PresetPattern::Numbers => write!(f, "numbers"),
            PresetPattern::Quoted => write!(f, "quoted"),
            PresetPattern::Bracketed => write!(f, "bracketed"),
            PresetPattern::Uuid => write!(f, "uuid"),
            PresetPattern::Ip => write!(f, "ip"),
        }
    }
}

impl std::str::FromStr for PresetPattern {
    type Err = Error;

    fn from_str(pattern: &str) -> Result<Self> {
        match pattern {
            "numbers" => Ok(PresetPattern::Numbers),
            "quoted" => Ok(PresetPattern::Quoted),
            "bracketed" => Ok(PresetPattern::Bracketed),
            "uuid" => Ok(PresetPattern::Uuid),
            "ip" => Ok(PresetPattern::Ip),
            _ => PresetPatternInvalidSnafu { pattern }.fail(),
        }
    }
}

impl PresetPattern {
    fn regex(&self) -> Regex {
        match self {
            PresetPattern::Numbers => Regex::new(r"\d+").unwrap(),
            PresetPattern::Quoted => Regex::new(r#"[\"'“”‘’][^\"'“”‘’]*[\"'“”‘’]"#).unwrap(),
            PresetPattern::Bracketed => Regex::new(r#"[({\[<「『【〔［｛〈《][^(){}\[\]<>「」『』【】〔〕［］｛｝〈〉《》]*[)}\]>」』】〕］｝〉》]"#).unwrap(),
            PresetPattern::Uuid => Regex::new(r"\b[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}\b").unwrap(),
            PresetPattern::Ip => Regex::new(r"((\d{1,3}\.){3}\d{1,3}(:\d+)?|(\[[0-9a-fA-F:]+\])(:\d+)?)").unwrap(),
        }
    }
}

#[derive(Debug, Default)]
pub struct DigestProcessorBuilder {
    fields: Fields,
    patterns: Vec<Regex>,
    ignore_missing: bool,
}

impl ProcessorBuilder for DigestProcessorBuilder {
    fn output_keys(&self) -> HashSet<&str> {
        self.fields
            .iter()
            .map(|f| f.target_or_input_field())
            .collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.fields.iter().map(|f| f.input_field()).collect()
    }

    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind> {
        self.build(intermediate_keys).map(ProcessorKind::Digest)
    }
}

impl DigestProcessorBuilder {
    fn build(self, intermediate_keys: &[String]) -> Result<DigestProcessor> {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input = OneInputOneOutputField::build(
                "digest",
                intermediate_keys,
                field.input_field(),
                field.target_or_input_field(),
            )?;
            real_fields.push(input);
        }
        Ok(DigestProcessor {
            fields: real_fields,
            ignore_missing: self.ignore_missing,
            patterns: self.patterns,
        })
    }
}

/// Computes a digest (hash) of the input string.
#[derive(Debug, Default)]
pub struct DigestProcessor {
    fields: Vec<OneInputOneOutputField>,
    ignore_missing: bool,
    patterns: Vec<Regex>,
}

impl DigestProcessor {
    fn remove_quoted_content(&self, val: &str) -> String {
        let re = Regex::new(r#""[^"]*""#).unwrap();
        re.replace_all(val, "").to_string()
    }

    fn process_string(&self, val: &str) -> Result<Value> {
        let mut input = val;
        let mut result = None;
        for pattern in self.patterns.iter() {
            let new_result = pattern.replace_all(input, "");
            if new_result != input {
                result = Some(new_result.to_string());
                input = result.as_ref().unwrap();
            }
        }

        if let Some(result) = result {
            Ok(Value::String(result))
        } else {
            Ok(Value::String(val.to_string()))
        }
    }

    fn process(&self, val: &Value) -> Result<Value> {
        match val {
            Value::String(val) => self.process_string(val),
            _ => ProcessorExpectStringSnafu {
                processor: PROCESSOR_DIGEST,
                v: val.clone(),
            }
            .fail(),
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for DigestProcessorBuilder {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;
        let mut patterns = Vec::new();

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
                PRESETS_PATTERNS_NAME => {
                    let preset_patterns: Vec<String> = v
                        .as_vec()
                        .with_context(|| PresetPatternInvalidSnafu {
                            pattern: key.to_string(),
                        })?
                        .iter()
                        .map(|p| p.as_str().unwrap().to_string())
                        .collect();
                    for pattern in preset_patterns {
                        let preset_pattern = pattern.parse::<PresetPattern>()?;
                        let regex = preset_pattern.regex();
                        patterns.push(regex);
                    }
                }
                REGEX_PATTERNS_NAME => {
                    let regex_patterns: Vec<String> = v
                        .as_vec()
                        .with_context(|| PresetPatternInvalidSnafu {
                            pattern: key.to_string(),
                        })?
                        .iter()
                        .map(|p| p.as_str().unwrap().to_string())
                        .collect();
                    for pattern in regex_patterns {
                        let regex = Regex::new(&pattern).unwrap();
                        patterns.push(regex);
                    }
                }
                _ => {}
            }
        }

        for field in fields.iter_mut() {
            field.target_field = Some(format!("{}_digest", field.input_field()));
        }

        Ok(DigestProcessorBuilder {
            fields,
            patterns,
            ignore_missing,
        })
    }
}

impl crate::etl::processor::Processor for DigestProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_DIGEST
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_name(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    let result = self.process(v)?;
                    let output_index = field.output_index();
                    val[output_index] = result;
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
    fn test_digest_processor_ip() {
        let processor = DigestProcessor {
            fields: vec![],
            ignore_missing: false,
            patterns: vec![PresetPattern::Ip.regex()],
        };

        let input = Value::String("192.168.1.1".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));
        let input = Value::String("192.168.1.1:8080".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]:8080".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("not an ip".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("not an ip".to_string()));
    }

    #[test]
    fn test_digest_processor_uuid() {
        let processor = DigestProcessor {
            fields: vec![],
            ignore_missing: false,
            patterns: vec![PresetPattern::Uuid.regex()],
        };
        // UUID v4
        let input = Value::String("123e4567-e89b-12d3-a456-426614174000".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // UUID v1
        let input = Value::String("6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // UUID v5
        let input = Value::String("886313e1-3b8a-5372-9b90-0c9aee199e5d".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // UUID with uppercase letters
        let input = Value::String("A987FBC9-4BED-3078-CF07-9141BA07C9F3".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // Negative case
        let input = Value::String("not a uuid".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("not a uuid".to_string()));
    }

    #[test]
    fn test_digest_processor_brackets() {
        let processor = DigestProcessor {
            fields: vec![],
            ignore_missing: false,
            patterns: vec![PresetPattern::Bracketed.regex()],
        };

        // Basic brackets
        let input = Value::String("[content]".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("(content)".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // Chinese brackets
        let input = Value::String("「content」".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("『content』".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("【content】".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // Unmatched/unclosed brackets should not match
        let input = Value::String("[content".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("[content".to_string()));

        let input = Value::String("content]".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("content]".to_string()));

        // Bad case
        let input = Value::String("[content}".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        // Negative case
        let input = Value::String("no brackets".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("no brackets".to_string()));
    }

    #[test]
    fn test_digest_processor_quotes() {
        let processor = DigestProcessor {
            fields: vec![],
            ignore_missing: false,
            patterns: vec![PresetPattern::Quoted.regex()],
        };

        let input = Value::String("\"quoted content\"".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("no quotes".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("no quotes".to_string()));
        let input = Value::String("".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }

    #[test]
    fn test_digest_processor_custom_regex() {
        let processor = DigestProcessor {
            fields: vec![],
            ignore_missing: false,
            patterns: vec![Regex::new(r"\d+").unwrap()],
        };

        let input = Value::String("12345".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));

        let input = Value::String("no digits".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("no digits".to_string()));
        let input = Value::String("".to_string());
        let result = processor.process(&input).unwrap();
        assert_eq!(result, Value::String("".to_string()));
    }
}
