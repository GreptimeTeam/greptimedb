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

use ahash::HashSet;
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
    fn fields_mut(&mut self) -> &mut Fields;
    fn kind(&self) -> &str;
    fn ignore_missing(&self) -> bool;

    fn ignore_processor_array_failure(&self) -> bool {
        true
    }

    fn output_keys(&self) -> HashSet<String>;

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

#[derive(Debug)]
pub enum ProcessorKind {
    CMCD(CMCDProcessor),
    Csv(CsvProcessor),
    Date(DateProcessor),
    Dissect(DissectProcessor),
    Epoch(EpochProcessor),
    Gsub(GsubProcessor),
    Join(JoinProcessor),
    Letter(LetterProcessor),
    Regex(RegexProcessor),
    UrlEncoding(UrlEncodingProcessor),
}

impl Clone for ProcessorKind {
    fn clone(&self) -> Self {
        todo!()
    }
}

impl Processor for ProcessorKind {
    fn fields(&self) -> &Fields {
        match self {
            ProcessorKind::CMCD(p) => p.fields(),
            ProcessorKind::Csv(p) => p.fields(),
            ProcessorKind::Date(p) => p.fields(),
            ProcessorKind::Dissect(p) => p.fields(),
            ProcessorKind::Epoch(p) => p.fields(),
            ProcessorKind::Gsub(p) => p.fields(),
            ProcessorKind::Join(p) => p.fields(),
            ProcessorKind::Letter(p) => p.fields(),
            ProcessorKind::Regex(p) => p.fields(),
            ProcessorKind::UrlEncoding(p) => p.fields(),
        }
    }

    fn kind(&self) -> &str {
        match self {
            ProcessorKind::CMCD(p) => p.kind(),
            ProcessorKind::Csv(p) => p.kind(),
            ProcessorKind::Date(p) => p.kind(),
            ProcessorKind::Dissect(p) => p.kind(),
            ProcessorKind::Epoch(p) => p.kind(),
            ProcessorKind::Gsub(p) => p.kind(),
            ProcessorKind::Join(p) => p.kind(),
            ProcessorKind::Letter(p) => p.kind(),
            ProcessorKind::Regex(p) => p.kind(),
            ProcessorKind::UrlEncoding(p) => p.kind(),
        }
    }

    fn ignore_missing(&self) -> bool {
        match self {
            ProcessorKind::CMCD(p) => p.ignore_missing(),
            ProcessorKind::Csv(p) => p.ignore_missing(),
            ProcessorKind::Date(p) => p.ignore_missing(),
            ProcessorKind::Dissect(p) => p.ignore_missing(),
            ProcessorKind::Epoch(p) => p.ignore_missing(),
            ProcessorKind::Gsub(p) => p.ignore_missing(),
            ProcessorKind::Join(p) => p.ignore_missing(),
            ProcessorKind::Letter(p) => p.ignore_missing(),
            ProcessorKind::Regex(p) => p.ignore_missing(),
            ProcessorKind::UrlEncoding(p) => p.ignore_missing(),
        }
    }

    fn output_keys(&self) -> HashSet<String> {
        match self {
            ProcessorKind::CMCD(p) => p.output_keys(),
            ProcessorKind::Csv(p) => p.output_keys(),
            ProcessorKind::Date(p) => p.output_keys(),
            ProcessorKind::Dissect(p) => p.output_keys(),
            ProcessorKind::Epoch(p) => p.output_keys(),
            ProcessorKind::Gsub(p) => p.output_keys(),
            ProcessorKind::Join(p) => p.output_keys(),
            ProcessorKind::Letter(p) => p.output_keys(),
            ProcessorKind::Regex(p) => p.output_keys(),
            ProcessorKind::UrlEncoding(p) => p.output_keys(),
        }
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match self {
            ProcessorKind::CMCD(p) => p.exec_field(val, field),
            ProcessorKind::Csv(p) => p.exec_field(val, field),
            ProcessorKind::Date(p) => p.exec_field(val, field),
            ProcessorKind::Dissect(p) => p.exec_field(val, field),
            ProcessorKind::Epoch(p) => p.exec_field(val, field),
            ProcessorKind::Gsub(p) => p.exec_field(val, field),
            ProcessorKind::Join(p) => p.exec_field(val, field),
            ProcessorKind::Letter(p) => p.exec_field(val, field),
            ProcessorKind::Regex(p) => p.exec_field(val, field),
            ProcessorKind::UrlEncoding(p) => p.exec_field(val, field),
        }
    }

    fn fields_mut(&mut self) -> &mut Fields {
        match self {
            ProcessorKind::CMCD(p) => p.fields_mut(),
            ProcessorKind::Csv(p) => p.fields_mut(),
            ProcessorKind::Date(p) => p.fields_mut(),
            ProcessorKind::Dissect(p) => p.fields_mut(),
            ProcessorKind::Epoch(p) => p.fields_mut(),
            ProcessorKind::Gsub(p) => p.fields_mut(),
            ProcessorKind::Join(p) => p.fields_mut(),
            ProcessorKind::Letter(p) => p.fields_mut(),
            ProcessorKind::Regex(p) => p.fields_mut(),
            ProcessorKind::UrlEncoding(p) => p.fields_mut(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Processors {
    pub processors: Vec<ProcessorKind>,
    pub required_keys: Vec<String>,
    pub output_keys: Vec<String>,
}

impl std::ops::Deref for Processors {
    type Target = Vec<ProcessorKind>;

    fn deref(&self) -> &Self::Target {
        &self.processors
    }
}

impl std::ops::DerefMut for Processors {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.processors
    }
}

impl Processors {
    pub fn required_keys(&self) -> &Vec<String> {
        &self.required_keys
    }

    pub fn output_keys(&self) -> &Vec<String> {
        &self.output_keys
    }
}

impl TryFrom<&Vec<yaml_rust::Yaml>> for Processors {
    type Error = String;

    fn try_from(vec: &Vec<yaml_rust::Yaml>) -> Result<Self, Self::Error> {
        let mut processors = vec![];
        let mut all_output_keys: Vec<String> = Vec::with_capacity(50);
        let mut all_required_keys = Vec::with_capacity(50);
        for doc in vec {
            let processor = parse_processor(doc)?;

            // get all required keys
            let processor_required_keys: Vec<String> =
                processor.fields().iter().map(|f| f.field.clone()).collect();
            // check all required keys are not in all_output_keys
            for key in &processor_required_keys {
                if !all_output_keys.contains(key) && !all_required_keys.contains(key) {
                    all_required_keys.push(key.clone());
                }
            }

            let mut processor_output_keys = processor.output_keys().into_iter().collect();
            all_output_keys.append(&mut processor_output_keys);

            processors.push(processor);
        }

        Ok(Processors {
            processors,
            required_keys: all_required_keys,
            output_keys: all_output_keys,
        })
    }
}

fn parse_processor(doc: &yaml_rust::Yaml) -> Result<ProcessorKind, String> {
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

    let processor = match str_key {
        cmcd::PROCESSOR_CMCD => ProcessorKind::CMCD(CMCDProcessor::try_from(value)?),
        csv::PROCESSOR_CSV => ProcessorKind::Csv(CsvProcessor::try_from(value)?),
        date::PROCESSOR_DATE => ProcessorKind::Date(DateProcessor::try_from(value)?),
        dissect::PROCESSOR_DISSECT => ProcessorKind::Dissect(DissectProcessor::try_from(value)?),
        epoch::PROCESSOR_EPOCH => ProcessorKind::Epoch(EpochProcessor::try_from(value)?),
        gsub::PROCESSOR_GSUB => ProcessorKind::Gsub(GsubProcessor::try_from(value)?),
        join::PROCESSOR_JOIN => ProcessorKind::Join(JoinProcessor::try_from(value)?),
        letter::PROCESSOR_LETTER => ProcessorKind::Letter(LetterProcessor::try_from(value)?),
        regex::PROCESSOR_REGEX => ProcessorKind::Regex(RegexProcessor::try_from(value)?),
        urlencoding::PROCESSOR_URL_ENCODING => {
            ProcessorKind::UrlEncoding(UrlEncodingProcessor::try_from(value)?)
        }
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
