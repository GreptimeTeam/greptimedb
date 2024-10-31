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
use snafu::{OptionExt, ResultExt};

use crate::etl::error::{
    EpochInvalidResolutionSnafu, Error, FailedToParseIntSnafu, KeyMustBeStringSnafu,
    ProcessorMissingFieldSnafu, ProcessorUnsupportedValueSnafu, Result,
};
use crate::etl::field::{Fields, OneInputOneOutputField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, ProcessorBuilder,
    ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::time::{
    MICROSECOND_RESOLUTION, MICRO_RESOLUTION, MILLISECOND_RESOLUTION, MILLI_RESOLUTION,
    MS_RESOLUTION, NANOSECOND_RESOLUTION, NANO_RESOLUTION, NS_RESOLUTION, SECOND_RESOLUTION,
    SEC_RESOLUTION, S_RESOLUTION, US_RESOLUTION,
};
use crate::etl::value::{Timestamp, Value};

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
    type Error = Error;

    fn try_from(s: &str) -> Result<Self> {
        match s {
            SECOND_RESOLUTION | SEC_RESOLUTION | S_RESOLUTION => Ok(Resolution::Second),
            MILLISECOND_RESOLUTION | MILLI_RESOLUTION | MS_RESOLUTION => Ok(Resolution::Milli),
            MICROSECOND_RESOLUTION | MICRO_RESOLUTION | US_RESOLUTION => Ok(Resolution::Micro),
            NANOSECOND_RESOLUTION | NANO_RESOLUTION | NS_RESOLUTION => Ok(Resolution::Nano),
            _ => EpochInvalidResolutionSnafu { resolution: s }.fail(),
        }
    }
}

#[derive(Debug, Default)]
pub struct EpochProcessorBuilder {
    fields: Fields,
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

    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind> {
        self.build(intermediate_keys).map(ProcessorKind::Epoch)
    }
}

impl EpochProcessorBuilder {
    pub fn build(self, intermediate_keys: &[String]) -> Result<EpochProcessor> {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input = OneInputOneOutputField::build(
                "epoch",
                intermediate_keys,
                field.input_field(),
                field.target_or_input_field(),
            )?;
            real_fields.push(input);
        }
        Ok(EpochProcessor {
            fields: real_fields,
            resolution: self.resolution,
            ignore_missing: self.ignore_missing,
        })
    }
}

/// support string, integer, float, time, epoch
/// deprecated it should be removed in the future
/// Reserved for compatibility only
#[derive(Debug, Default)]
pub struct EpochProcessor {
    fields: Vec<OneInputOneOutputField>,
    resolution: Resolution,
    ignore_missing: bool,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl EpochProcessor {
    fn parse(&self, val: &Value) -> Result<Timestamp> {
        let t: i64 = match val {
            Value::String(s) => s
                .parse::<i64>()
                .context(FailedToParseIntSnafu { value: s })?,
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
                return ProcessorUnsupportedValueSnafu {
                    processor: PROCESSOR_EPOCH,
                    val: val.to_string(),
                }
                .fail();
            }
        };

        match self.resolution {
            Resolution::Second => Ok(Timestamp::Second(t)),
            Resolution::Milli => Ok(Timestamp::Millisecond(t)),
            Resolution::Micro => Ok(Timestamp::Microsecond(t)),
            Resolution::Nano => Ok(Timestamp::Nanosecond(t)),
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for EpochProcessorBuilder {
    type Error = Error;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut resolution = Resolution::default();
        let mut ignore_missing = false;

        for (k, v) in hash {
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
        let processor = EpochProcessor {
            resolution: super::Resolution::Second,
            ..Default::default()
        };

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
