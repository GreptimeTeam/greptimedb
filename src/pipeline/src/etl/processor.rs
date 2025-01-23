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
pub mod decolorize;
pub mod digest;
pub mod dissect;
pub mod epoch;
pub mod gsub;
pub mod join;
pub mod json_path;
pub mod letter;
pub mod regex;
pub mod timestamp;
pub mod urlencoding;

use std::collections::BTreeMap;

use ahash::{HashSet, HashSetExt};
use cmcd::{CmcdProcessor, CmcdProcessorBuilder};
use csv::{CsvProcessor, CsvProcessorBuilder};
use date::{DateProcessor, DateProcessorBuilder};
use decolorize::{DecolorizeProcessor, DecolorizeProcessorBuilder};
use digest::{DigestProcessor, DigestProcessorBuilder};
use dissect::{DissectProcessor, DissectProcessorBuilder};
use enum_dispatch::enum_dispatch;
use epoch::{EpochProcessor, EpochProcessorBuilder};
use gsub::{GsubProcessor, GsubProcessorBuilder};
use itertools::Itertools;
use join::{JoinProcessor, JoinProcessorBuilder};
use json_path::{JsonPathProcessor, JsonPathProcessorBuilder};
use letter::{LetterProcessor, LetterProcessorBuilder};
use regex::{RegexProcessor, RegexProcessorBuilder};
use snafu::{OptionExt, ResultExt};
use timestamp::{TimestampProcessor, TimestampProcessorBuilder};
use urlencoding::{UrlEncodingProcessor, UrlEncodingProcessorBuilder};

use super::error::{
    FailedParseFieldFromStringSnafu, FieldMustBeTypeSnafu, ProcessorKeyMustBeStringSnafu,
    ProcessorMustBeMapSnafu, ProcessorMustHaveStringKeySnafu, UnsupportedProcessorSnafu,
};
use super::field::{Field, Fields};
use crate::etl::error::{Error, Result};
use crate::etl::value::Value;

const FIELD_NAME: &str = "field";
const FIELDS_NAME: &str = "fields";
const IGNORE_MISSING_NAME: &str = "ignore_missing";
const METHOD_NAME: &str = "method";
const PATTERN_NAME: &str = "pattern";
const PATTERNS_NAME: &str = "patterns";
const SEPARATOR_NAME: &str = "separator";
const TARGET_FIELDS_NAME: &str = "target_fields";
const JSON_PATH_NAME: &str = "json_path";
const JSON_PATH_RESULT_INDEX_NAME: &str = "result_index";

/// Processor trait defines the interface for all processors.
///
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
    fn exec_mut(&self, val: &mut BTreeMap<String, Value>) -> Result<()>;
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
    JsonPath(JsonPathProcessor),
    Decolorize(DecolorizeProcessor),
    Digest(DigestProcessor),
}

/// ProcessorBuilder trait defines the interface for all processor builders
/// A processor builder is used to create a processor
#[enum_dispatch(ProcessorBuilders)]
pub trait ProcessorBuilder: std::fmt::Debug + Send + Sync + 'static {
    /// Get the processor's output keys
    fn output_keys(&self) -> HashSet<&str>;
    /// Get the processor's input keys
    fn input_keys(&self) -> HashSet<&str>;
    /// Build the processor
    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind>;
}

#[derive(Debug, Default)]
pub struct Processors {
    /// A ordered list of processors
    /// The order of processors is important
    /// The output of the first processor will be the input of the second processor
    pub processors: Vec<ProcessorKind>,
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

impl TryFrom<&Vec<yaml_rust::Yaml>> for Processors {
    type Error = Error;

    fn try_from(vec: &Vec<yaml_rust::Yaml>) -> Result<Self> {
        let mut processors_builders = vec![];
        for doc in vec {
            let processor = parse_processor(doc)?;
            processors_builders.push(processor);
        }
        Ok(Processors {
            processors: processors_builders,
        })
    }
}

fn parse_processor(doc: &yaml_rust::Yaml) -> Result<ProcessorKind> {
    let map = doc.as_hash().context(ProcessorMustBeMapSnafu)?;

    let key = map.keys().next().context(ProcessorMustHaveStringKeySnafu)?;

    let value = map
        .get(key)
        .unwrap()
        .as_hash()
        .context(ProcessorMustBeMapSnafu)?;

    let str_key = key.as_str().context(ProcessorKeyMustBeStringSnafu)?;

    todo!()
}

pub(crate) fn yaml_string(v: &yaml_rust::Yaml, field: &str) -> Result<String> {
    v.as_str()
        .map(|s| s.to_string())
        .context(FieldMustBeTypeSnafu {
            field,
            ty: "string",
        })
}

pub(crate) fn yaml_strings(v: &yaml_rust::Yaml, field: &str) -> Result<Vec<String>> {
    let vec = v
        .as_vec()
        .context(FieldMustBeTypeSnafu {
            field,
            ty: "list of string",
        })?
        .iter()
        .map(|v| v.as_str().unwrap_or_default().into())
        .collect();
    Ok(vec)
}

pub(crate) fn yaml_bool(v: &yaml_rust::Yaml, field: &str) -> Result<bool> {
    v.as_bool().context(FieldMustBeTypeSnafu {
        field,
        ty: "boolean",
    })
}

pub(crate) fn yaml_parse_string<T>(v: &yaml_rust::Yaml, field: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    yaml_string(v, field)?
        .parse::<T>()
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
        .context(FailedParseFieldFromStringSnafu { field })
}

pub(crate) fn yaml_parse_strings<T>(v: &yaml_rust::Yaml, field: &str) -> Result<Vec<T>>
where
    T: std::str::FromStr,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    yaml_strings(v, field).and_then(|v| {
        v.into_iter()
            .map(|s| {
                s.parse::<T>()
                    .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                    .context(FailedParseFieldFromStringSnafu { field })
            })
            .collect()
    })
}

pub(crate) fn yaml_new_fields(v: &yaml_rust::Yaml, field: &str) -> Result<Fields> {
    yaml_parse_strings(v, field).map(Fields::new)
}

pub(crate) fn yaml_new_field(v: &yaml_rust::Yaml, field: &str) -> Result<Field> {
    yaml_parse_string(v, field)
}
