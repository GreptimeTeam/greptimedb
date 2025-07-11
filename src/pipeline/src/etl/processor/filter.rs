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

use ahash::{HashSet, HashSetExt};
use snafu::OptionExt;
use vrl::prelude::Value as VrlValue;

use crate::error::{
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
    ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, yaml_strings, FIELDS_NAME, FIELD_NAME,
};
use crate::Processor;

pub(crate) const PROCESSOR_FILTER: &str = "filter";

const MATCH_MODE_NAME: &str = "mode";
const MATCH_OP_NAME: &str = "match_op";
const CASE_INSENSITIVE_NAME: &str = "case_insensitive";
const TARGETS_NAME: &str = "targets";

#[derive(Debug)]
enum MatchMode {
    SimpleMatch(MatchOp),
}

impl Default for MatchMode {
    fn default() -> Self {
        Self::SimpleMatch(MatchOp::default())
    }
}

#[derive(Debug, Default)]
enum MatchOp {
    #[default]
    In,
    NotIn,
}

/// Filter out the whole line if matches.
/// Ultimately it's a condition check, maybe we can use VRL to do more complex check.
/// Implement simple string match for now. Can be extended later.
#[derive(Debug, Default)]
pub struct FilterProcessor {
    fields: Fields,
    mode: MatchMode,
    case_insensitive: bool,
    targets: HashSet<String>,
}

impl TryFrom<&yaml_rust::yaml::Hash> for FilterProcessor {
    type Error = Error;

    // match mode can be extended in the future
    #[allow(clippy::single_match)]
    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut mode = MatchMode::default();
        let mut op = MatchOp::default();
        let mut case_insensitive = true;
        let mut targets = HashSet::new();

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                FIELD_NAME => fields = Fields::one(yaml_new_field(v, FIELD_NAME)?),
                FIELDS_NAME => fields = yaml_new_fields(v, FIELDS_NAME)?,
                MATCH_MODE_NAME => match yaml_string(v, MATCH_MODE_NAME)?.as_str() {
                    "simple" => mode = MatchMode::SimpleMatch(MatchOp::In),
                    _ => {}
                },
                MATCH_OP_NAME => match yaml_string(v, MATCH_OP_NAME)?.as_str() {
                    "in" => op = MatchOp::In,
                    "not_in" => op = MatchOp::NotIn,
                    _ => {}
                },
                CASE_INSENSITIVE_NAME => case_insensitive = yaml_bool(v, CASE_INSENSITIVE_NAME)?,
                TARGETS_NAME => {
                    yaml_strings(v, TARGETS_NAME)?
                        .into_iter()
                        .filter(|s| !s.is_empty())
                        .for_each(|s| {
                            targets.insert(s);
                        });
                }
                _ => {}
            }
        }

        if matches!(mode, MatchMode::SimpleMatch(_)) {
            mode = MatchMode::SimpleMatch(op);
        }

        if targets.is_empty() {
            return ProcessorMissingFieldSnafu {
                processor: PROCESSOR_FILTER,
                field: TARGETS_NAME.to_string(),
            }
            .fail();
        }

        if case_insensitive {
            targets = targets.into_iter().map(|s| s.to_lowercase()).collect();
        }

        Ok(FilterProcessor {
            fields,
            mode,
            case_insensitive,
            targets,
        })
    }
}

impl FilterProcessor {
    fn match_target(&self, input: &str) -> bool {
        let input = if self.case_insensitive {
            &input.to_lowercase()
        } else {
            input
        };

        match &self.mode {
            MatchMode::SimpleMatch(op) => match op {
                MatchOp::In => self.targets.contains(input),
                MatchOp::NotIn => !self.targets.contains(input),
            },
        }
    }
}

impl Processor for FilterProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_FILTER
    }

    fn ignore_missing(&self) -> bool {
        true
    }

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            let index = field.input_field();
            match val.get(index) {
                Some(VrlValue::Bytes(b)) => {
                    if self.match_target(&String::from_utf8_lossy(b)) {
                        return Ok(VrlValue::Null);
                    }
                }
                Some(v) => {
                    return ProcessorExpectStringSnafu {
                        processor: self.kind(),
                        v: v.clone(),
                    }
                    .fail();
                }
                None => {}
            }
        }

        Ok(val)
    }
}

#[cfg(test)]
mod test {
    use ahash::HashSet;
    use vrl::prelude::{Bytes, Value as VrlValue};
    use vrl::value::{KeyString, ObjectMap};

    use crate::etl::field::{Field, Fields};
    use crate::etl::processor::filter::{FilterProcessor, MatchMode, MatchOp};
    use crate::Processor;

    #[test]
    fn test_eq() {
        let processor = FilterProcessor {
            fields: Fields::one(Field::new("name", None)),
            mode: MatchMode::SimpleMatch(MatchOp::In),
            case_insensitive: false,
            targets: HashSet::from_iter(vec!["John".to_string()]),
        };

        let val = VrlValue::Object(ObjectMap::from_iter(vec![(
            KeyString::from("name"),
            VrlValue::Bytes(Bytes::from("John")),
        )]));

        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, VrlValue::Null);

        let val = VrlValue::Object(ObjectMap::from_iter(vec![(
            KeyString::from("name"),
            VrlValue::Bytes(Bytes::from("Wick")),
        )]));
        let expect = val.clone();
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, expect);
    }

    #[test]
    fn test_ne() {
        let processor = FilterProcessor {
            fields: Fields::one(Field::new("name", None)),
            mode: MatchMode::SimpleMatch(MatchOp::NotIn),
            case_insensitive: false,
            targets: HashSet::from_iter(vec!["John".to_string()]),
        };

        let val = VrlValue::Object(ObjectMap::from_iter(vec![(
            KeyString::from("name"),
            VrlValue::Bytes(Bytes::from("John")),
        )]));
        let expect = val.clone();
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, expect);

        let val = VrlValue::Object(ObjectMap::from_iter(vec![(
            KeyString::from("name"),
            VrlValue::Bytes(Bytes::from("Wick")),
        )]));
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, VrlValue::Null);
    }

    #[test]
    fn test_case() {
        let processor = FilterProcessor {
            fields: Fields::one(Field::new("name", None)),
            mode: MatchMode::SimpleMatch(MatchOp::In),
            case_insensitive: true,
            targets: HashSet::from_iter(vec!["john".to_string()]),
        };

        let val = VrlValue::Object(ObjectMap::from_iter(vec![(
            KeyString::from("name"),
            VrlValue::Bytes(Bytes::from("JoHN")),
        )]));
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, VrlValue::Null);
    }
}
