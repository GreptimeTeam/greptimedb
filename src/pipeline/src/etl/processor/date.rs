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

use std::sync::Arc;

use ahash::HashSet;
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Tz;
use lazy_static::lazy_static;
use snafu::{OptionExt, ResultExt};

use crate::etl::error::{
    DateFailedToGetLocalTimezoneSnafu, DateFailedToGetTimestampSnafu, DateParseSnafu,
    DateParseTimezoneSnafu, Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu,
    ProcessorFailedToParseStringSnafu, ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::{Fields, OneInputOneOutputField};
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, yaml_strings, Processor,
    ProcessorBuilder, ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::{Timestamp, Value};

pub(crate) const PROCESSOR_DATE: &str = "date";

const FORMATS_NAME: &str = "formats"; // default RFC3339
const TIMEZONE_NAME: &str = "timezone"; // default UTC
const LOCALE_NAME: &str = "locale";
const OUTPUT_FORMAT_NAME: &str = "output_format"; // default with input format

lazy_static! {
    static ref DEFAULT_FORMATS: Vec<Arc<String>> = vec![
                    // timezone with colon
                    "%Y-%m-%dT%H:%M:%S%:z",
                    "%Y-%m-%dT%H:%M:%S%.3f%:z",
                    "%Y-%m-%dT%H:%M:%S%.6f%:z",
                    "%Y-%m-%dT%H:%M:%S%.9f%:z",
                    // timezone without colon
                    "%Y-%m-%dT%H:%M:%S%z",
                    "%Y-%m-%dT%H:%M:%S%.3f%z",
                    "%Y-%m-%dT%H:%M:%S%.6f%z",
                    "%Y-%m-%dT%H:%M:%S%.9f%z",
                    // without timezone
                    "%Y-%m-%dT%H:%M:%SZ",
                    "%Y-%m-%dT%H:%M:%S",
                    "%Y-%m-%dT%H:%M:%S%.3f",
                    "%Y-%m-%dT%H:%M:%S%.6f",
                    "%Y-%m-%dT%H:%M:%S%.9f",
                ]
                .iter()
                .map(|s| Arc::new(s.to_string()))
                .collect();
}

#[derive(Debug)]
struct Formats(Vec<Arc<String>>);

impl Default for Formats {
    fn default() -> Self {
        Formats(DEFAULT_FORMATS.clone())
    }
}

impl Formats {
    fn new(mut formats: Vec<Arc<String>>) -> Self {
        formats.sort();
        formats.dedup();
        Formats(formats)
    }
}

impl std::ops::Deref for Formats {
    type Target = Vec<Arc<String>>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug, Default)]
pub struct DateProcessorBuilder {
    fields: Fields,
    formats: Formats,
    timezone: Option<Arc<String>>,
    locale: Option<Arc<String>>,
    ignore_missing: bool,
}

impl ProcessorBuilder for DateProcessorBuilder {
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
        self.build(intermediate_keys).map(ProcessorKind::Date)
    }
}

impl DateProcessorBuilder {
    pub fn build(self, intermediate_keys: &[String]) -> Result<DateProcessor> {
        let mut real_fields = vec![];
        for field in self.fields.into_iter() {
            let input = OneInputOneOutputField::build(
                "date",
                intermediate_keys,
                field.input_field(),
                field.target_or_input_field(),
            )?;
            real_fields.push(input);
        }
        Ok(DateProcessor {
            fields: real_fields,
            formats: self.formats,
            timezone: self.timezone,
            locale: self.locale,
            ignore_missing: self.ignore_missing,
        })
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for DateProcessorBuilder {
    type Error = Error;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut formats = Formats::default();
        let mut timezone = None;
        let mut locale = None;
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

                FORMATS_NAME => {
                    let format_strs = yaml_strings(v, FORMATS_NAME)?;
                    if format_strs.is_empty() {
                        formats = Formats::new(DEFAULT_FORMATS.clone());
                    } else {
                        formats = Formats::new(format_strs.into_iter().map(Arc::new).collect());
                    }
                }
                TIMEZONE_NAME => {
                    timezone = Some(Arc::new(yaml_string(v, TIMEZONE_NAME)?));
                }
                LOCALE_NAME => {
                    locale = Some(Arc::new(yaml_string(v, LOCALE_NAME)?));
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }

                _ => {}
            }
        }

        let builder = DateProcessorBuilder {
            fields,
            formats,
            timezone,
            locale,
            ignore_missing,
        };

        Ok(builder)
    }
}

/// deprecated it should be removed in the future
/// Reserved for compatibility only
#[derive(Debug, Default)]
pub struct DateProcessor {
    fields: Vec<OneInputOneOutputField>,
    formats: Formats,
    timezone: Option<Arc<String>>,
    locale: Option<Arc<String>>, // to support locale

    ignore_missing: bool,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl DateProcessor {
    fn parse(&self, val: &str) -> Result<Timestamp> {
        let mut tz = Tz::UTC;
        if let Some(timezone) = &self.timezone {
            tz = timezone.parse::<Tz>().context(DateParseTimezoneSnafu {
                value: timezone.as_ref(),
            })?;
        }

        for fmt in self.formats.iter() {
            if let Ok(ns) = try_parse(val, fmt, tz) {
                return Ok(Timestamp::Nanosecond(ns));
            }
        }

        ProcessorFailedToParseStringSnafu {
            kind: PROCESSOR_DATE.to_string(),
            value: val.to_string(),
        }
        .fail()
    }
}

impl Processor for DateProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_DATE
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::String(s)) => {
                    let timestamp = self.parse(s)?;
                    let output_index = field.output_index();
                    val[output_index] = Value::Timestamp(timestamp);
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind().to_string(),
                            field: field.input_name().to_string(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    return ProcessorExpectStringSnafu {
                        processor: self.kind().to_string(),
                        v: v.clone(),
                    }
                    .fail();
                }
            }
        }
        Ok(())
    }
}

/// try to parse val with timezone first, if failed, parse without timezone
fn try_parse(val: &str, fmt: &str, tz: Tz) -> Result<i64> {
    if let Ok(dt) = DateTime::parse_from_str(val, fmt) {
        Ok(dt
            .timestamp_nanos_opt()
            .context(DateFailedToGetTimestampSnafu)?)
    } else {
        let dt = NaiveDateTime::parse_from_str(val, fmt)
            .context(DateParseSnafu { value: val })?
            .and_local_timezone(tz)
            .single()
            .context(DateFailedToGetLocalTimezoneSnafu)?;
        Ok(dt
            .timestamp_nanos_opt()
            .context(DateFailedToGetTimestampSnafu)?)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use chrono_tz::Asia::Tokyo;

    use crate::etl::processor::date::{try_parse, DateProcessor};

    #[test]
    fn test_try_parse() {
        let time_with_tz = "2014-5-17T04:34:56+00:00";
        let fmt_with_tz = "%Y-%m-%dT%H:%M:%S%:z";

        let time_without_tz = "2014-5-17T13:34:56";
        let fmt_without_tz = "%Y-%m-%dT%H:%M:%S";

        let tz = Tokyo;

        let parsed_with_tz = try_parse(time_with_tz, fmt_with_tz, tz);
        assert!(parsed_with_tz.is_ok());

        let parsed_without_tz = try_parse(time_without_tz, fmt_without_tz, tz);
        assert!(parsed_without_tz.is_ok());

        assert_eq!(parsed_with_tz.unwrap(), parsed_without_tz.unwrap());
    }

    #[test]
    fn test_parse() {
        let processor = DateProcessor::default();

        let values: Vec<&str> = vec![
            "2014-5-17T12:34:56",
            "2014-5-17T12:34:56Z",
            "2014-5-17T12:34:56+09:30",
            "2014-5-17T12:34:56.000+09:30",
            "2014-5-17T12:34:56-0930",
            "2014-5-17T12:34:56.000-0930",
        ]
        .into_iter()
        .collect();

        for value in values {
            let parsed = processor.parse(value);
            assert!(parsed.is_ok());
        }
    }

    #[test]
    fn test_parse_with_formats() {
        let formats = vec![
            "%Y-%m-%dT%H:%M:%S%:z",
            "%Y-%m-%dT%H:%M:%S%.3f%:z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        .into_iter()
        .map(|s| Arc::new(s.to_string()))
        .collect::<Vec<_>>();
        let processor = DateProcessor {
            formats: super::Formats(formats),
            ..Default::default()
        };

        let values: Vec<&str> = vec![
            "2014-5-17T12:34:56",
            "2014-5-17T12:34:56Z",
            "2014-5-17T12:34:56+09:30",
            "2014-5-17T12:34:56.000+09:30",
            "2014-5-17T12:34:56-0930",
            "2014-5-17T12:34:56.000-0930",
        ]
        .into_iter()
        .collect();

        for value in values {
            let parsed = processor.parse(value);
            assert!(parsed.is_ok());
        }
    }

    #[test]
    fn test_parse_with_timezone() {
        let processor = DateProcessor {
            timezone: Some(Arc::new("Asia/Tokyo".to_string())),
            ..Default::default()
        };

        let values: Vec<&str> = vec![
            "2014-5-17T12:34:56",
            "2014-5-17T12:34:56Z",
            "2014-5-17T12:34:56+09:30",
            "2014-5-17T12:34:56.000+09:30",
            "2014-5-17T12:34:56-0930",
            "2014-5-17T12:34:56.000-0930",
        ]
        .into_iter()
        .collect();

        for value in values {
            let parsed = processor.parse(value);
            assert!(parsed.is_ok());
        }
    }
}
