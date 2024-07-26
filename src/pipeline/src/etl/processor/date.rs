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

use std::borrow::Cow;
use std::sync::Arc;

use ahash::HashSet;
use chrono::{DateTime, NaiveDateTime};
use chrono_tz::Tz;
use lazy_static::lazy_static;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    update_one_one_output_keys, yaml_bool, yaml_field, yaml_fields, yaml_string, yaml_strings,
    Processor, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::{Map, Time, Value};

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

#[derive(Debug, Default)]
struct Formats(Vec<Arc<String>>);

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
pub struct DateProcessor {
    fields: Fields,

    formats: Formats,
    timezone: Option<Arc<String>>,
    locale: Option<Arc<String>>, // to support locale
    output_format: Option<Arc<String>>,

    ignore_missing: bool,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl DateProcessor {
    fn with_fields(&mut self, mut fields: Fields) {
        update_one_one_output_keys(&mut fields);
        self.fields = fields
    }

    fn with_formats(&mut self, v: Option<Vec<Arc<String>>>) {
        let v = match v {
            Some(v) if !v.is_empty() => v,
            _ => DEFAULT_FORMATS.clone(),
        };

        let formats = Formats::new(v);
        self.formats = formats;
    }

    fn with_timezone(&mut self, timezone: String) {
        if !timezone.is_empty() {
            self.timezone = Some(Arc::new(timezone));
        }
    }

    fn with_locale(&mut self, locale: String) {
        if !locale.is_empty() {
            self.locale = Some(Arc::new(locale));
        }
    }

    fn with_output_format(&mut self, output_format: String) {
        if !output_format.is_empty() {
            self.output_format = Some(Arc::new(output_format));
        }
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn parse(&self, val: &str) -> Result<Time, String> {
        let mut tz = Tz::UTC;
        if let Some(timezone) = &self.timezone {
            tz = timezone.parse::<Tz>().map_err(|e| e.to_string())?;
        }

        for fmt in self.formats.iter() {
            if let Ok(ns) = try_parse(val, fmt, tz) {
                let mut t = Time::new(Cow::Borrowed(val), ns);
                t.with_format(Some(fmt.clone()));
                t.with_timezone(self.timezone.clone());
                return Ok(t);
            }
        }

        Err(format!("{} processor: failed to parse {val}", self.kind(),))
    }

    fn process_field(&self, val: &str, field: &Field) -> Result<Map, String> {
        let key = field.get_target_field();

        Ok(Map::one(key, Value::Time(self.parse(val)?)))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for DateProcessor {
    type Error = String;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = DateProcessor::default();

        let mut formats_opt = None;

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

                FORMATS_NAME => {
                    let formats = yaml_strings(v, FORMATS_NAME)?;
                    formats_opt = Some(formats.into_iter().map(Arc::new).collect());
                }
                TIMEZONE_NAME => {
                    processor.with_timezone(yaml_string(v, TIMEZONE_NAME)?);
                }
                LOCALE_NAME => {
                    processor.with_locale(yaml_string(v, LOCALE_NAME)?);
                }
                OUTPUT_FORMAT_NAME => {
                    processor.with_output_format(yaml_string(v, OUTPUT_FORMAT_NAME)?);
                }

                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }

                _ => {}
            }
        }

        processor.with_formats(formats_opt);

        Ok(processor)
    }
}

impl Processor for DateProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_DATE
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
            .map(|f| f.get_target_field().to_string())
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match val {
            Value::String(s) => self.process_field(s, field),
            _ => Err(format!(
                "{} processor: expect string value, but got {val:?}",
                self.kind()
            )),
        }
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields().iter() {
            let index = field.input_field.index;
            match val.get(index) {
                Some(Value::String(s)) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let mut map = self.process_field(s, field)?;
                    field.output_fields_index_mapping.iter().for_each(|(k, output_index)| {
                        if let Some(v) = map.remove(k) {
                            val[*output_index] = v;
                        }
                    });
                }
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
                    return Err(format!(
                        "{} processor: expect string value, but got {v:?}",
                        self.kind()
                    ));
                }
            }
        }
        Ok(())
    }

    fn ignore_processor_array_failure(&self) -> bool {
        true
    }
}

/// try to parse val with timezone first, if failed, parse without timezone
fn try_parse(val: &str, fmt: &str, tz: Tz) -> Result<i64, String> {
    if let Ok(dt) = DateTime::parse_from_str(val, fmt) {
        Ok(dt.timestamp_nanos_opt().ok_or("failed to get timestamp")?)
    } else {
        let dt = NaiveDateTime::parse_from_str(val, fmt)
            .map_err(|e| e.to_string())?
            .and_local_timezone(tz)
            .single()
            .ok_or("failed to get local timezone")?;
        Ok(dt.timestamp_nanos_opt().ok_or("failed to get timestamp")?)
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
        let mut processor = DateProcessor::default();
        processor.with_formats(None);

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
        let mut processor = DateProcessor::default();
        let formats = vec![
            "%Y-%m-%dT%H:%M:%S%:z",
            "%Y-%m-%dT%H:%M:%S%.3f%:z",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ]
        .into_iter()
        .map(|s| Arc::new(s.to_string()))
        .collect();
        processor.with_formats(Some(formats));

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
        let mut processor = DateProcessor::default();
        processor.with_formats(None);
        processor.with_timezone("Asia/Tokyo".to_string());

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
