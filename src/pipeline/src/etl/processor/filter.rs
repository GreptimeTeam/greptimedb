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
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
    ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, FIELDS_NAME, FIELD_NAME,
};
use crate::{Processor, Value};

pub(crate) const PROCESSOR_FILTER: &str = "filter";

const MATCH_MODE_NAME: &str = "mode";
const MATCH_OP_NAME: &str = "match_op";
const CASE_INSENSITIVE_NAME: &str = "case_insensitive";
const TARGET_NAME: &str = "target";

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
    Eq,
    Ne,
}

/// Filter out the whole line if matches.
/// Ultimately it's a condition check, maybe we can use VRL to do more complex check.
/// Implement simple string match for now. Can be extended later.
#[derive(Debug, Default)]
pub struct FilterProcessor {
    fields: Fields,
    mode: MatchMode,
    case_insensitive: bool,
    target: String,
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
        let mut target = String::new();

        for (k, v) in value.iter() {
            let key = k
                .as_str()
                .with_context(|| KeyMustBeStringSnafu { k: k.clone() })?;
            match key {
                FIELD_NAME => fields = Fields::one(yaml_new_field(v, FIELD_NAME)?),
                FIELDS_NAME => fields = yaml_new_fields(v, FIELDS_NAME)?,
                MATCH_MODE_NAME => match yaml_string(v, MATCH_MODE_NAME)?.as_str() {
                    "simple" => mode = MatchMode::SimpleMatch(MatchOp::Eq),
                    _ => {}
                },
                MATCH_OP_NAME => match yaml_string(v, MATCH_OP_NAME)?.as_str() {
                    "eq" => op = MatchOp::Eq,
                    "ne" => op = MatchOp::Ne,
                    _ => {}
                },
                CASE_INSENSITIVE_NAME => case_insensitive = yaml_bool(v, CASE_INSENSITIVE_NAME)?,
                TARGET_NAME => target = yaml_string(v, TARGET_NAME)?,
                _ => {}
            }
        }

        if matches!(mode, MatchMode::SimpleMatch(_)) {
            mode = MatchMode::SimpleMatch(op);
        }

        if target.is_empty() {
            return ProcessorMissingFieldSnafu {
                processor: PROCESSOR_FILTER,
                field: TARGET_NAME.to_string(),
            }
            .fail();
        }

        if case_insensitive {
            target = target.to_lowercase();
        }

        Ok(FilterProcessor {
            fields,
            mode,
            case_insensitive,
            target,
        })
    }
}

impl FilterProcessor {
    fn match_target(&self, input: String) -> bool {
        let input = if self.case_insensitive {
            input.to_lowercase()
        } else {
            input
        };

        match &self.mode {
            MatchMode::SimpleMatch(op) => match op {
                MatchOp::Eq => input == self.target,
                MatchOp::Ne => input != self.target,
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

    fn exec_mut(&self, mut val: Value) -> Result<Value> {
        let v_map = val.as_map_mut().context(ValueMustBeMapSnafu)?;

        for field in self.fields.iter() {
            let index = field.input_field();
            match v_map.get(index) {
                Some(Value::String(s)) => {
                    if self.match_target(s.clone()) {
                        return Ok(Value::Null);
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
    use crate::etl::field::{Field, Fields};
    use crate::etl::processor::filter::{FilterProcessor, MatchMode, MatchOp};
    use crate::{Map, Processor, Value};

    #[test]
    fn test_eq() {
        let processor = FilterProcessor {
            fields: Fields::one(Field::new("name", None)),
            mode: MatchMode::SimpleMatch(MatchOp::Eq),
            case_insensitive: false,
            target: "John".to_string(),
        };

        let val = Value::Map(Map::one("name", Value::String("John".to_string())));

        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, Value::Null);

        let val = Value::Map(Map::one("name", Value::String("Wick".to_string())));
        let expect = val.clone();
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, expect);
    }

    #[test]
    fn test_ne() {
        let processor = FilterProcessor {
            fields: Fields::one(Field::new("name", None)),
            mode: MatchMode::SimpleMatch(MatchOp::Ne),
            case_insensitive: false,
            target: "John".to_string(),
        };

        let val = Value::Map(Map::one("name", Value::String("John".to_string())));
        let expect = val.clone();
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, expect);

        let val = Value::Map(Map::one("name", Value::String("Wick".to_string())));
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_case() {
        let processor = FilterProcessor {
            fields: Fields::one(Field::new("name", None)),
            mode: MatchMode::SimpleMatch(MatchOp::Eq),
            case_insensitive: true,
            target: "john".to_string(),
        };

        let val = Value::Map(Map::one("name", Value::String("JoHN".to_string())));
        let result = processor.exec_mut(val).unwrap();
        assert_eq!(result, Value::Null);
    }
}
