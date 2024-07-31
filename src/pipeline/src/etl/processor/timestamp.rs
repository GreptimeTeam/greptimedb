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

use super::yaml_strings;
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

pub(crate) const PROCESSOR_TIMESTAMP: &str = "timestamp";
const RESOLUTION_NAME: &str = "resolution";
const FORMATS_NAME: &str = "formats"; // default RFC3339

lazy_static! {
    static ref DEFAULT_FORMATS: Vec<(Arc<String>,Tz)> = vec![
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
                .map(|s| (Arc::new(s.to_string()),Tz::UCT))
                .collect();
}

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
struct Formats(Vec<(Arc<String>, Tz)>);

impl Formats {
    fn new(mut formats: Vec<(Arc<String>, Tz)>) -> Self {
        formats.sort_by_key(|(key, _)| key.clone());
        formats.dedup();
        Formats(formats)
    }
}

impl std::ops::Deref for Formats {
    type Target = Vec<(Arc<String>, Tz)>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// support string, integer, float, time, epoch
#[derive(Debug, Default)]
pub struct TimestampProcessor {
    fields: Fields,
    formats: Formats,
    resolution: Resolution,
    ignore_missing: bool,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl TimestampProcessor {
    fn with_fields(&mut self, mut fields: Fields) {
        update_one_one_output_keys(&mut fields);
        self.fields = fields
    }

    fn with_resolution(&mut self, resolution: Resolution) {
        self.resolution = resolution;
    }

    fn with_formats(&mut self, v: Option<Vec<(Arc<String>, Tz)>>) {
        let v = match v {
            Some(v) if !v.is_empty() => v,
            _ => DEFAULT_FORMATS.clone(),
        };

        let formats = Formats::new(v);
        self.formats = formats;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
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

    fn parse_time_str(&self, val: &str) -> Result<i64, String> {
        for (fmt, tz) in self.formats.iter() {
            if let Ok(ns) = Self::try_parse(val, fmt, tz.clone()) {
                return Ok(ns);
            }
        }
        Err(format!("{} processor: failed to parse {val}", self.kind(),))
    }

    fn parse(&self, val: &Value) -> Result<Epoch, String> {
        let t: i64 = match val {
            Value::String(s) => {
                let t = s.parse::<i64>();
                match t {
                    Ok(t) => t,
                    Err(_) => {
                        let ns = self.parse_time_str(s)?;
                        return Ok(Epoch::Nanosecond(ns));
                    }
                }
            }
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

            Value::Epoch(e) => match self.resolution {
                Resolution::Second => e.timestamp(),
                Resolution::Milli => e.timestamp_millis(),
                Resolution::Micro => e.timestamp_micros(),
                Resolution::Nano => e.timestamp_nanos(),
            },

            _ => {
                return Err(format!(
                    "{PROCESSOR_TIMESTAMP} processor: unsupported value {val}"
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
        let key = field.get_target_field();

        Ok(Map::one(key, Value::Epoch(self.parse(val)?)))
    }
}

fn parse_formats(yaml: &yaml_rust::yaml::Yaml) -> Result<Vec<(Arc<String>, Tz)>, String> {
    return match yaml.as_vec() {
        Some(formats_yaml) => {
            let mut formats = Vec::with_capacity(formats_yaml.len());
            for v in formats_yaml {
                let s = yaml_strings(v, FORMATS_NAME)
                    .or(yaml_string(v, FORMATS_NAME).map(|s| vec![s]))?;
                if s.len() != 1 && s.len() != 2 {
                    return Err(format!(
                        "{PROCESSOR_TIMESTAMP} processor: invalid format {s:?}"
                    ));
                }
                let mut iter = s.into_iter();
                // safety: unwrap is safe here
                let formatter = iter.next().unwrap();
                let tz = iter
                    .next()
                    .map(|tz| tz.parse::<Tz>().ok())
                    .flatten()
                    .unwrap_or(Tz::UCT);
                formats.push((Arc::new(formatter), tz));
            }
            Ok(formats)
        }
        None => Err(format!(
            "{PROCESSOR_TIMESTAMP} processor: invalid format {yaml:?}"
        )),
    };
}

impl TryFrom<&yaml_rust::yaml::Hash> for TimestampProcessor {
    type Error = String;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = TimestampProcessor::default();

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
                    let formats = parse_formats(v)?;
                    processor.with_formats(Some(formats));
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

impl Processor for TimestampProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_TIMESTAMP
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
                    field
                        .output_fields_index_mapping
                        .iter()
                        .for_each(|(k, output_index)| {
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
    use yaml_rust::YamlLoader;

    use super::TimestampProcessor;
    use crate::etl::value::{Epoch, Value};

    #[test]
    fn test_parse_epoch() {
        let processor_yaml_str = r#"fields:
  - hello
resolution: s
formats:
  - "%Y-%m-%dT%H:%M:%S%:z"
  - "%Y-%m-%dT%H:%M:%S%.3f%:z"
  - "%Y-%m-%dT%H:%M:%S"
  - "%Y-%m-%dT%H:%M:%SZ"
"#;
        let yaml = &YamlLoader::load_from_str(processor_yaml_str).unwrap()[0];
        let timestamp_yaml = yaml.as_hash().unwrap();
        let processor = TimestampProcessor::try_from(timestamp_yaml).unwrap();

        let values = [
            (
                Value::String("1573840000".into()),
                Epoch::Second(1573840000),
            ),
            (Value::Int32(1573840001), Epoch::Second(1573840001)),
            (Value::Uint64(1573840002), Epoch::Second(1573840002)),
            // float32 has a problem expressing the timestamp.
            // 1573840003.0_f32 as i64 is 1573840000
            //(Value::Float32(1573840003.0), Epoch::Second(1573840003)),
            (
                Value::String("2019-11-15T17:46:40Z".into()),
                Epoch::Nanosecond(1573840000000000000),
            ),
        ];

        for (value, result) in values {
            let parsed = processor.parse(&value).unwrap();
            assert_eq!(parsed, result);
        }
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
            let parsed = processor.parse(&Value::String(value.into()));
            assert!(parsed.is_ok());
        }
    }

    #[test]
    fn test_parse_with_timezone() {
        let processor_yaml_str = r#"fields:
  - hello
resolution: s
formats:
  - ["%Y-%m-%dT%H:%M:%S%:z", "Asia/Tokyo"]
  - ["%Y-%m-%dT%H:%M:%S%.3f%:z", "Asia/Tokyo"]
  - ["%Y-%m-%dT%H:%M:%S", "Asia/Tokyo"]
  - ["%Y-%m-%dT%H:%M:%SZ", "Asia/Tokyo"]
"#;
        let yaml = &YamlLoader::load_from_str(processor_yaml_str).unwrap()[0];
        let timestamp_yaml = yaml.as_hash().unwrap();
        let processor = TimestampProcessor::try_from(timestamp_yaml).unwrap();

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
            let parsed = processor.parse(&Value::String(value.into()));
            assert!(parsed.is_ok());
        }
    }
}
