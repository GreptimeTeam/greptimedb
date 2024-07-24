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

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    update_one_one_output_keys, yaml_bool, yaml_field, yaml_fields, yaml_string, Processor,
    FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::time::{
    MICROSECOND_RESOLUTION, MICRO_RESOLUTION, MILLISECOND_RESOLUTION, MILLI_RESOLUTION,
    MS_RESOLUTION, NANOSECOND_RESOLUTION, NANO_RESOLUTION, NS_RESOLUTION, SECOND_RESOLUTION,
    SEC_RESOLUTION, S_RESOLUTION, US_RESOLUTION,
};
use crate::etl::value::{Epoch, Map, Value};

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

/// support string, integer, float, time, epoch
#[derive(Debug, Default)]
pub struct EpochProcessor {
    fields: Fields,
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
        update_one_one_output_keys(&mut fields);
        self.fields = fields
    }

    fn with_resolution(&mut self, resolution: Resolution) {
        self.resolution = resolution;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn parse(&self, val: &Value) -> Result<Epoch, String> {
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

            Value::Time(t) => match self.resolution {
                Resolution::Second => t.timestamp(),
                Resolution::Milli => t.timestamp_millis(),
                Resolution::Micro => t.timestamp_micros(),
                Resolution::Nano => t.timestamp_nanos(),
            },

            Value::Epoch(e) => match self.resolution {
                Resolution::Second => e.timestamp(),
                Resolution::Milli => e.timestamp_millis(),
                Resolution::Micro => e.timestamp_micros(),
                Resolution::Nano => e.timestamp_nanos(),
            },

            _ => {
                return Err(format!(
                    "{PROCESSOR_EPOCH} processor: unsupported value {val}"
                ))
            }
        };

        match self.resolution {
            Resolution::Second => Ok(Epoch::Second(t)),
            Resolution::Milli => Ok(Epoch::Millisecond(t)),
            Resolution::Micro => Ok(Epoch::Microsecond(t)),
            Resolution::Nano => Ok(Epoch::Nanosecond(t)),
        }
    }

    fn process_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        let key = field.get_renamed_field();

        Ok(Map::one(key, Value::Epoch(self.parse(val)?)))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for EpochProcessor {
    type Error = String;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = EpochProcessor::default();

        for (k, v) in hash {
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
                RESOLUTION_NAME => {
                    let s = yaml_string(v, RESOLUTION_NAME)?.as_str().try_into()?;
                    processor.with_resolution(s);
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

impl Processor for EpochProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_EPOCH
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
            .map(|f| f.get_renamed_field().to_string())
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        self.process_field(val, field)
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            let index = field.input_field.index;
            match val.get(index) {
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return Err(format!(
                            "{} processor: missing field: {}",
                            self.kind(),
                            field.get_field_name()
                        ));
                    }
                }
                Some(v) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let mut map = self.process_field(v, field)?;
                    field.output_fields.iter().for_each(|(k, output_index)| {
                        if let Some(v) = map.remove(k) {
                            val[*output_index] = v;
                        }
                    });
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
            assert_eq!(parsed, super::Epoch::Second(1573840000));
        }
    }
}
