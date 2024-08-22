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
pub mod timestamp;
pub mod urlencoding;

use ahash::{HashSet, HashSetExt};
use cmcd::{CmcdProcessor, CmcdProcessorBuilder};
use csv::{CsvProcessor, CsvProcessorBuilder};
use date::{DateProcessor, DateProcessorBuilder};
use dissect::{DissectProcessor, DissectProcessorBuilder};
use enum_dispatch::enum_dispatch;
use epoch::{EpochProcessor, EpochProcessorBuilder};
use gsub::{GsubProcessor, GsubProcessorBuilder};
use itertools::Itertools;
use join::{JoinProcessor, JoinProcessorBuilder};
use letter::{LetterProcessor, LetterProcessorBuilder};
use regex::{RegexProcessor, RegexProcessorBuilder};
use timestamp::{TimestampProcessor, TimestampProcessorBuilder};
use urlencoding::{UrlEncodingProcessor, UrlEncodingProcessorBuilder};

use super::field::{NewField, NewFields};
use crate::etl::field::{Field, Fields};
use crate::etl::value::Value;

const FIELD_NAME: &str = "field";
const FIELDS_NAME: &str = "fields";
const IGNORE_MISSING_NAME: &str = "ignore_missing";
const METHOD_NAME: &str = "method";
const PATTERN_NAME: &str = "pattern";
const PATTERNS_NAME: &str = "patterns";
const SEPARATOR_NAME: &str = "separator";
const TARGET_FIELDS_NAME: &str = "target_fields";

// const IF_NAME: &str = "if";
// const IGNORE_FAILURE_NAME: &str = "ignore_failure";
// const ON_FAILURE_NAME: &str = "on_failure";
// const TAG_NAME: &str = "tag";

/// Processor trait defines the interface for all processors
/// A processor is a transformation that can be applied to a field in a document
/// It can be used to extract, transform, or enrich data
/// Now Processor only have one input field. In the future, we may support multiple input fields.
/// The output of a processor is a map of key-value pairs that will be merged into the document when you use exec_map method.
#[enum_dispatch(ProcessorKind)]
pub trait Processor: std::fmt::Debug + Send + Sync + 'static {
    /// Get the processor's kind
    fn kind(&self) -> &str;

    /// Whether to ignore missing
    fn ignore_missing(&self) -> bool;

    /// Execute the processor on a vector which be preprocessed by the pipeline
    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String>;
}

#[derive(Debug)]
#[enum_dispatch]
pub enum ProcessorKind {
    Cmcd(CmcdProcessor),
    Csv(CsvProcessor),
    Dissect(DissectProcessor),
    Gsub(GsubProcessor),
    Join(JoinProcessor),
    Letter(LetterProcessor),
    Regex(RegexProcessor),
    Timestamp(TimestampProcessor),
    UrlEncoding(UrlEncodingProcessor),
    Epoch(EpochProcessor),
    Date(DateProcessor),
}

#[enum_dispatch(ProcessorBuilders)]
pub trait ProcessorBuilder: std::fmt::Debug + Send + Sync + 'static {
    fn output_keys(&self) -> HashSet<&str>;
    fn input_keys(&self) -> HashSet<&str>;
    fn build(self, intermediate_keys: &[String]) -> ProcessorKind;
}

#[derive(Debug)]
#[enum_dispatch]
pub enum ProcessorBuilders {
    Cmcd(CmcdProcessorBuilder),
    Csv(CsvProcessorBuilder),
    Dissect(DissectProcessorBuilder),
    Gsub(GsubProcessorBuilder),
    Join(JoinProcessorBuilder),
    Letter(LetterProcessorBuilder),
    Regex(RegexProcessorBuilder),
    Timestamp(TimestampProcessorBuilder),
    UrlEncoding(UrlEncodingProcessorBuilder),
    Epoch(EpochProcessorBuilder),
    Date(DateProcessorBuilder),
}

#[derive(Debug, Default)]
pub struct ProcessorBuilderList {
    pub(crate) processor_builders: Vec<ProcessorBuilders>,
    pub(crate) input_keys: Vec<String>,
    pub(crate) output_keys: Vec<String>,
    pub(crate) original_input_keys: Vec<String>,
}

#[derive(Debug, Default)]
pub struct Processors {
    /// A ordered list of processors
    /// The order of processors is important
    /// The output of the first processor will be the input of the second processor
    pub processors: Vec<ProcessorKind>,
    /// all required keys in all processors
    pub required_keys: Vec<String>,
    /// all required keys in user-supplied data, not pipeline output fields
    pub required_original_keys: Vec<String>,
    /// all output keys in all processors
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
    /// A collection of all the processor's required input fields
    pub fn required_keys(&self) -> &Vec<String> {
        &self.required_keys
    }

    /// A collection of all the processor's output fields
    pub fn output_keys(&self) -> &Vec<String> {
        &self.output_keys
    }

    /// Required fields in user-supplied data, not pipeline output fields.
    pub fn required_original_keys(&self) -> &Vec<String> {
        &self.required_original_keys
    }
}

impl TryFrom<&Vec<yaml_rust::Yaml>> for ProcessorBuilderList {
    type Error = String;

    fn try_from(vec: &Vec<yaml_rust::Yaml>) -> Result<Self, Self::Error> {
        let mut processors_builders = vec![];
        let mut all_output_keys = HashSet::with_capacity(50);
        let mut all_required_keys = HashSet::with_capacity(50);
        let mut all_required_original_keys = HashSet::with_capacity(50);
        for doc in vec {
            let processor = parse_processor(doc)?;
            processors_builders.push(processor);
        }

        for processor in processors_builders.iter() {
            {
                // get all required keys
                let processor_required_keys = processor.input_keys();

                for key in &processor_required_keys {
                    if !all_output_keys.contains(key) {
                        all_required_original_keys.insert(*key);
                    }
                }

                all_required_keys.extend(processor_required_keys);

                let processor_output_keys = processor.output_keys().into_iter();
                all_output_keys.extend(processor_output_keys);
            }
        }

        let all_required_keys = all_required_keys
            .into_iter()
            .map(|x| x.to_string())
            .sorted()
            .collect();
        let all_output_keys = all_output_keys
            .into_iter()
            .map(|x| x.to_string())
            .sorted()
            .collect();
        let all_required_original_keys = all_required_original_keys
            .into_iter()
            .map(|x| x.to_string())
            .sorted()
            .collect();

        Ok(ProcessorBuilderList {
            processor_builders: processors_builders,
            input_keys: all_required_keys,
            output_keys: all_output_keys,
            original_input_keys: all_required_original_keys,
        })
    }
}

fn parse_processor(doc: &yaml_rust::Yaml) -> Result<ProcessorBuilders, String> {
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
        cmcd::PROCESSOR_CMCD => ProcessorBuilders::Cmcd(CmcdProcessorBuilder::try_from(value)?),
        csv::PROCESSOR_CSV => ProcessorBuilders::Csv(CsvProcessorBuilder::try_from(value)?),
        dissect::PROCESSOR_DISSECT => {
            ProcessorBuilders::Dissect(DissectProcessorBuilder::try_from(value)?)
        }
        epoch::PROCESSOR_EPOCH => ProcessorBuilders::Epoch(EpochProcessorBuilder::try_from(value)?),
        date::PROCESSOR_DATE => ProcessorBuilders::Date(DateProcessorBuilder::try_from(value)?),
        gsub::PROCESSOR_GSUB => ProcessorBuilders::Gsub(GsubProcessorBuilder::try_from(value)?),
        join::PROCESSOR_JOIN => ProcessorBuilders::Join(JoinProcessorBuilder::try_from(value)?),
        letter::PROCESSOR_LETTER => {
            ProcessorBuilders::Letter(LetterProcessorBuilder::try_from(value)?)
        }
        regex::PROCESSOR_REGEX => ProcessorBuilders::Regex(RegexProcessorBuilder::try_from(value)?),
        timestamp::PROCESSOR_TIMESTAMP => {
            ProcessorBuilders::Timestamp(TimestampProcessorBuilder::try_from(value)?)
        }
        urlencoding::PROCESSOR_URL_ENCODING => {
            ProcessorBuilders::UrlEncoding(UrlEncodingProcessorBuilder::try_from(value)?)
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

pub(crate) fn yaml_new_fileds(v: &yaml_rust::Yaml, field: &str) -> Result<NewFields, String> {
    yaml_parse_strings(v, field).map(NewFields::new)
}

pub(crate) fn yaml_new_field(v: &yaml_rust::Yaml, field: &str) -> Result<NewField, String> {
    yaml_parse_string(v, field)
}

pub(crate) fn yaml_field(v: &yaml_rust::Yaml, field: &str) -> Result<Field, String> {
    yaml_parse_string(v, field)
}

pub(crate) fn update_one_one_output_keys(fields: &mut Fields) {
    for field in fields.iter_mut() {
        field
            .output_fields_index_mapping
            .insert(field.get_target_field().to_string(), 0_usize);
    }
}
