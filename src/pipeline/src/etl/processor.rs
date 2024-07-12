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

pub mod cmcd;
pub mod csv;
pub mod date;
pub mod dissect;
pub mod epoch;
pub mod gsub;
pub mod join;
pub mod letter;
pub mod regex;
pub mod urlencoding;

use std::sync::Arc;

use cmcd::CMCDProcessor;
use csv::CsvProcessor;
use date::DateProcessor;
use dissect::DissectProcessor;
use epoch::EpochProcessor;
use gsub::GsubProcessor;
use join::JoinProcessor;
use letter::LetterProcessor;
use regex::RegexProcessor;
use urlencoding::UrlEncodingProcessor;

use crate::etl::field::{Field, Fields};
use crate::etl::value::{Map, Value};

const FIELD_NAME: &str = "field";
const FIELDS_NAME: &str = "fields";
const IGNORE_MISSING_NAME: &str = "ignore_missing";
const METHOD_NAME: &str = "method";
const PATTERN_NAME: &str = "pattern";
const PATTERNS_NAME: &str = "patterns";
const SEPARATOR_NAME: &str = "separator";

// const IF_NAME: &str = "if";
// const IGNORE_FAILURE_NAME: &str = "ignore_failure";
// const ON_FAILURE_NAME: &str = "on_failure";
// const TAG_NAME: &str = "tag";

pub trait Processor: std::fmt::Debug + Send + Sync + 'static {
    fn fields(&self) -> &Fields;
    fn kind(&self) -> &str;
    fn ignore_missing(&self) -> bool;

    fn ignore_processor_array_failure(&self) -> bool {
        true
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String>;

    fn exec_map<'a>(&self, map: &'a mut Map) -> Result<&'a mut Map, String> {
        for ff @ Field { field, .. } in self.fields().iter() {
            match map.get(field) {
                Some(v) => {
                    map.extend(self.exec_field(v, ff)?);
                }
                None if self.ignore_missing() => {}
                None => {
                    return Err(format!(
                        "{} processor: field '{field}' is required but missing in {map}",
                        self.kind(),
                    ))
                }
            }
        }
        
        Ok(map)
    }
}

#[derive(Debug, Default, Clone)]
pub struct Processors {
    pub processors: Vec<Arc<dyn Processor>>,
}

impl Processors {
    pub fn new() -> Self {
        Processors { processors: vec![] }
    }
}

impl std::ops::Deref for Processors {
    type Target = Vec<Arc<dyn Processor>>;

    fn deref(&self) -> &Self::Target {
        &self.processors
    }
}

impl TryFrom<&Vec<yaml_rust::Yaml>> for Processors {
    type Error = String;

    fn try_from(vec: &Vec<yaml_rust::Yaml>) -> Result<Self, Self::Error> {
        let mut processors = vec![];

        for doc in vec {
            processors.push(parse_processor(doc)?);
        }

        Ok(Processors { processors })
    }
}

fn parse_processor(doc: &yaml_rust::Yaml) -> Result<Arc<dyn Processor>, String> {
    let map = doc.as_hash().ok_or("processor must be a map".to_string())?;

    let key = map
        .keys()
        .next()
        .ok_or("processor must have a string key".to_string())?;

    let value = map
        .get(key)
        .unwrap()
        .as_hash()
        .expect("processor value must be a map");

    let str_key = key
        .as_str()
        .ok_or("processor key must be a string".to_string())?;

    let processor: Arc<dyn Processor> = match str_key {
        cmcd::PROCESSOR_CMCD => Arc::new(CMCDProcessor::try_from(value)?),
        csv::PROCESSOR_CSV => Arc::new(CsvProcessor::try_from(value)?),
        date::PROCESSOR_DATE => Arc::new(DateProcessor::try_from(value)?),
        dissect::PROCESSOR_DISSECT => Arc::new(DissectProcessor::try_from(value)?),
        epoch::PROCESSOR_EPOCH => Arc::new(EpochProcessor::try_from(value)?),
        gsub::PROCESSOR_GSUB => Arc::new(GsubProcessor::try_from(value)?),
        join::PROCESSOR_JOIN => Arc::new(JoinProcessor::try_from(value)?),
        letter::PROCESSOR_LETTER => Arc::new(LetterProcessor::try_from(value)?),
        regex::PROCESSOR_REGEX => Arc::new(RegexProcessor::try_from(value)?),
        urlencoding::PROCESSOR_URL_ENCODING => Arc::new(UrlEncodingProcessor::try_from(value)?),
        _ => return Err(format!("unsupported {} processor", str_key)),
    };

    Ok(processor)
}

pub(crate) fn yaml_string(v: &yaml_rust::Yaml, field: &str) -> Result<String, String> {
    v.as_str()
        .map(|s| s.to_string())
        .ok_or(format!("'{field}' must be a string"))
}

pub(crate) fn yaml_strings(v: &yaml_rust::Yaml, field: &str) -> Result<Vec<String>, String> {
    let vec = v
        .as_vec()
        .ok_or(format!("'{field}' must be a list of strings",))?
        .iter()
        .map(|v| v.as_str().unwrap_or_default().into())
        .collect();
    Ok(vec)
}

pub(crate) fn yaml_bool(v: &yaml_rust::Yaml, field: &str) -> Result<bool, String> {
    v.as_bool().ok_or(format!("'{field}' must be a boolean"))
}

pub(crate) fn yaml_parse_string<T>(v: &yaml_rust::Yaml, field: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: ToString,
{
    yaml_string(v, field)?
        .parse::<T>()
        .map_err(|e| e.to_string())
}

pub(crate) fn yaml_parse_strings<T>(v: &yaml_rust::Yaml, field: &str) -> Result<Vec<T>, String>
where
    T: std::str::FromStr,
    T::Err: ToString,
{
    yaml_strings(v, field).and_then(|v| {
        v.into_iter()
            .map(|s| s.parse::<T>().map_err(|e| e.to_string()))
            .collect()
    })
}

pub(crate) fn yaml_fields(v: &yaml_rust::Yaml, field: &str) -> Result<Fields, String> {
    let v = yaml_parse_strings(v, field)?;
    Fields::new(v)
}

pub(crate) fn yaml_field(v: &yaml_rust::Yaml, field: &str) -> Result<Field, String> {
    yaml_parse_string(v, field)
}
