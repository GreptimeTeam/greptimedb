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
    ProcessorBuilder, ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::time::{
    MICROSECOND_RESOLUTION, MICRO_RESOLUTION, MILLISECOND_RESOLUTION, MILLI_RESOLUTION,
    MS_RESOLUTION, NANOSECOND_RESOLUTION, NANO_RESOLUTION, NS_RESOLUTION, SECOND_RESOLUTION,
    SEC_RESOLUTION, S_RESOLUTION, US_RESOLUTION,
};
use crate::etl::value::{Map, Timestamp, Value};

pub(crate) const PROCESSOR_EPOCH: &str = "epoch";
const RESOLUTION_NAME: &str = "resolution";

#[derive(Debug, Default)]
enum Resolution {
    Second,
    #[default]
    Milli,
    Micro,
    Nano,
}

impl TryFrom<&str> for Resolution {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        match s {
            SECOND_RESOLUTION | SEC_RESOLUTION | S_RESOLUTION => Ok(Resolution::Second),
            MILLISECOND_RESOLUTION | MILLI_RESOLUTION | MS_RESOLUTION => Ok(Resolution::Milli),
            MICROSECOND_RESOLUTION | MICRO_RESOLUTION | US_RESOLUTION => Ok(Resolution::Micro),
            NANOSECOND_RESOLUTION | NANO_RESOLUTION | NS_RESOLUTION => Ok(Resolution::Nano),
            _ => Err(format!("invalid resolution: {s}")),
        }
    }
}

#[derive(Debug, Default)]
pub struct EpochProcessorBuilder {
    fields: NewFields,
    resolution: Resolution,
    ignore_missing: bool,
}

impl ProcessorBuilder for EpochProcessorBuilder {
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
        let builder = Self::build(self, intermediate_keys);
        ProcessorKind::Epoch(builder)
    }
}

impl EpochProcessorBuilder {
    pub fn build(self, intermediate_keys: &[String]) -> EpochProcessor {
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
        EpochProcessor {
            real_fields,
            resolution: self.resolution,
            ignore_missing: self.ignore_missing,
        }
    }
}

/// support string, integer, float, time, epoch
/// deprecated it should be removed in the future
/// Reserved for compatibility only
#[derive(Debug, Default)]
pub struct EpochProcessor {
    real_fields: Vec<OneInputOneOutPutField>,
    resolution: Resolution,
    ignore_missing: bool,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl EpochProcessor {
    fn with_fields(&mut self, mut fields: Fields) {
        todo!()
        // update_one_one_output_keys(&mut fields);
        // self.fields = fields
    }

    fn with_resolution(&mut self, resolution: Resolution) {
        self.resolution = resolution;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn parse(&self, val: &Value) -> Result<Timestamp, String> {
        let t: i64 = match val {
            Value::String(s) => s
                .parse::<i64>()
                .map_err(|e| format!("Failed to parse {} to number: {}", s, e))?,
            Value::Int16(i) => *i as i64,
            Value::Int32(i) => *i as i64,
            Value::Int64(i) => *i,
            Value::Uint8(i) => *i as i64,
            Value::Uint16(i) => *i as i64,
            Value::Uint32(i) => *i as i64,
            Value::Uint64(i) => *i as i64,
            Value::Float32(f) => *f as i64,
            Value::Float64(f) => *f as i64,

            Value::Timestamp(t) => match self.resolution {
                Resolution::Second => t.timestamp(),
                Resolution::Milli => t.timestamp_millis(),
                Resolution::Micro => t.timestamp_micros(),
                Resolution::Nano => t.timestamp_nanos(),
            },

            _ => {
                return Err(format!(
                    "{PROCESSOR_EPOCH} processor: unsupported value {val}"
                ))
            }
        };

        match self.resolution {
            Resolution::Second => Ok(Timestamp::Second(t)),
            Resolution::Milli => Ok(Timestamp::Millisecond(t)),
            Resolution::Micro => Ok(Timestamp::Microsecond(t)),
            Resolution::Nano => Ok(Timestamp::Nanosecond(t)),
        }
    }

    fn process_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        let key = field.get_target_field();

        Ok(Map::one(key, Value::Timestamp(self.parse(val)?)))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for EpochProcessorBuilder {
    type Error = String;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut fields = NewFields::default();
        let mut resolution = Resolution::default();
        let mut ignore_missing = false;

        for (k, v) in hash {
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
                RESOLUTION_NAME => {
                    let s = yaml_string(v, RESOLUTION_NAME)?.as_str().try_into()?;
                    resolution = s;
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }

                _ => {}
            }
        }
        let builder = EpochProcessorBuilder {
            fields,
            resolution,
            ignore_missing,
        };

        Ok(builder)
    }
}

impl Processor for EpochProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_EPOCH
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.real_fields.iter() {
            let index = field.input_index();
            match val.get(index) {
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
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let timestamp = self.parse(v)?;
                    let output_index = field.output_index();
                    val[output_index] = Value::Timestamp(timestamp);
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::EpochProcessor;
    use crate::etl::value::Value;

    #[test]
    fn test_parse_epoch() {
        let mut processor = EpochProcessor::default();
        processor.with_resolution(super::Resolution::Second);

        let values = [
            Value::String("1573840000".into()),
            Value::Int32(1573840000),
            Value::Uint64(1573840000),
            Value::Float32(1573840000.0),
        ];

        for value in values {
            let parsed = processor.parse(&value).unwrap();
            assert_eq!(parsed, super::Timestamp::Second(1573840000));
        }
    }
}
