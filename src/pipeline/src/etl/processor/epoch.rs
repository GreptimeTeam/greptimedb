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

use chrono::{DateTime, Utc};
use common_time::timestamp::TimeUnit;
use snafu::{OptionExt, ResultExt};
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    EpochInvalidResolutionSnafu, Error, FailedToParseIntSnafu, InvalidEpochForResolutionSnafu,
    KeyMustBeStringSnafu, ProcessorMissingFieldSnafu, ProcessorUnsupportedValueSnafu, Result,
    ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME,
};
use crate::etl::value::{
    MICROSECOND_RESOLUTION, MICRO_RESOLUTION, MILLISECOND_RESOLUTION, MILLI_RESOLUTION,
    MS_RESOLUTION, NANOSECOND_RESOLUTION, NANO_RESOLUTION, NS_RESOLUTION, SECOND_RESOLUTION,
    SEC_RESOLUTION, S_RESOLUTION, US_RESOLUTION,
};

pub(crate) const PROCESSOR_EPOCH: &str = "epoch";
const RESOLUTION_NAME: &str = "resolution";

#[derive(Debug, Default)]
pub(crate) enum Resolution {
    Second,
    #[default]
    Milli,
    Micro,
    Nano,
}

impl std::fmt::Display for Resolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Resolution::Second => SECOND_RESOLUTION,
            Resolution::Milli => MILLISECOND_RESOLUTION,
            Resolution::Micro => MICROSECOND_RESOLUTION,
            Resolution::Nano => NANOSECOND_RESOLUTION,
        };
        write!(f, "{}", text)
    }
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

impl From<&Resolution> for TimeUnit {
    fn from(resolution: &Resolution) -> Self {
        match resolution {
            Resolution::Second => TimeUnit::Second,
            Resolution::Milli => TimeUnit::Millisecond,
            Resolution::Micro => TimeUnit::Microsecond,
            Resolution::Nano => TimeUnit::Nanosecond,
        }
    }
}

/// support string, integer, float, time, epoch
/// deprecated it should be removed in the future
/// Reserved for compatibility only
#[derive(Debug, Default)]
pub struct EpochProcessor {
    pub(crate) fields: Fields,
    pub(crate) resolution: Resolution,
    ignore_missing: bool,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl EpochProcessor {
    fn parse(&self, val: &VrlValue) -> Result<DateTime<Utc>> {
        let t: i64 =
            match val {
                VrlValue::Bytes(bytes) => String::from_utf8_lossy(bytes).parse::<i64>().context(
                    FailedToParseIntSnafu {
                        value: val.to_string_lossy(),
                    },
                )?,
                VrlValue::Integer(ts) => *ts,
                VrlValue::Float(not_nan) => not_nan.into_inner() as i64,
                VrlValue::Timestamp(date_time) => return Ok(*date_time),
                _ => {
                    return ProcessorUnsupportedValueSnafu {
                        processor: PROCESSOR_EPOCH,
                        val: val.to_string(),
                    }
                    .fail();
                }
            };

        match self.resolution {
            Resolution::Second => DateTime::from_timestamp(t, 0),
            Resolution::Milli => DateTime::from_timestamp_millis(t),
            Resolution::Micro => DateTime::from_timestamp_micros(t),
            Resolution::Nano => Some(DateTime::from_timestamp_nanos(t)),
        }
        .context(InvalidEpochForResolutionSnafu {
            value: t,
            resolution: self.resolution.to_string(),
        })
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for EpochProcessor {
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
        let builder = EpochProcessor {
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

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            match val.get(index) {
                Some(VrlValue::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    let timestamp = self.parse(v)?;
                    let output_index = field.target_or_input_field();
                    val.insert(
                        KeyString::from(output_index.to_string()),
                        VrlValue::Timestamp(timestamp),
                    );
                }
            }
        }
        Ok(val)
    }
}

#[cfg(test)]
mod tests {
    use chrono::DateTime;
    use ordered_float::NotNan;
    use vrl::prelude::Bytes;
    use vrl::value::Value as VrlValue;

    use super::EpochProcessor;

    #[test]
    fn test_parse_epoch() {
        let processor = EpochProcessor {
            resolution: super::Resolution::Second,
            ..Default::default()
        };

        let values = [
            VrlValue::Bytes(Bytes::from("1573840000")),
            VrlValue::Integer(1573840000),
            VrlValue::Integer(1573840000),
            VrlValue::Float(NotNan::new(1573840000.0).unwrap()),
        ];

        for value in values {
            let parsed = processor.parse(&value).unwrap();
            assert_eq!(parsed, DateTime::from_timestamp(1573840000, 0).unwrap());
        }
    }
}
