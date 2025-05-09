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
pub mod json_parse;
pub mod json_path;
pub mod letter;
pub mod regex;
pub mod select;
pub mod simple_extract;
pub mod timestamp;
pub mod urlencoding;

use std::str::FromStr;

use cmcd::CmcdProcessor;
use csv::CsvProcessor;
use date::DateProcessor;
use decolorize::DecolorizeProcessor;
use digest::DigestProcessor;
use dissect::DissectProcessor;
use enum_dispatch::enum_dispatch;
use epoch::EpochProcessor;
use gsub::GsubProcessor;
use join::JoinProcessor;
use json_path::JsonPathProcessor;
use letter::LetterProcessor;
use regex::RegexProcessor;
use snafu::{OptionExt, ResultExt};
use timestamp::TimestampProcessor;
use urlencoding::UrlEncodingProcessor;

use crate::error::{
    Error, FailedParseFieldFromStringSnafu, FieldMustBeTypeSnafu, InvalidFieldRenameSnafu,
    ProcessorKeyMustBeStringSnafu, ProcessorMustBeMapSnafu, ProcessorMustHaveStringKeySnafu,
    Result, UnsupportedProcessorSnafu,
};
use crate::etl::field::{Field, Fields};
use crate::etl::processor::json_parse::JsonParseProcessor;
use crate::etl::processor::select::SelectProcessor;
use crate::etl::processor::simple_extract::SimpleExtractProcessor;
use crate::etl::PipelineMap;

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
const KEY_NAME: &str = "key";
const TYPE_NAME: &str = "type";
const RENAME_TO_KEY: &str = "rename_to";

/// Macro to extract a string value from a YAML map
#[macro_export]
macro_rules! yaml_map_get_str {
    ($map:expr, $key:expr, $value:expr) => {
        $map.get(&yaml_rust::Yaml::String($key.to_string()))
            .and_then(|v| v.as_str())
            .with_context(|| InvalidFieldRenameSnafu {
                value: $value.clone(),
            })
    };
}

lazy_static::lazy_static! {
    static ref STRING_FN: fn(&str, &yaml_rust::Yaml) -> Result<String> = |_, v| {
        Ok(v.as_str().unwrap_or_default().into())
    };

    static ref STRING_OR_HASH_FN: fn(&str, &yaml_rust::Yaml) -> Result<Field> = |field, v| {
        match v {
            yaml_rust::Yaml::String(s) => Field::from_str(s),
            yaml_rust::Yaml::Hash(m) => {
                let key = yaml_map_get_str!(m, KEY_NAME, v)?;
                let rename_to = yaml_map_get_str!(m, RENAME_TO_KEY, v)?;
                Ok(Field::new(key, Some(rename_to.to_string())))
            }
            _ => FieldMustBeTypeSnafu {
                field,
                ty: "string or key-rename_to map",
            }
            .fail(),
        }
    };
}

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
    fn exec_mut(&self, val: &mut PipelineMap) -> Result<()>;
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
    JsonParse(JsonParseProcessor),
    SimpleJsonPath(SimpleExtractProcessor),
    Decolorize(DecolorizeProcessor),
    Digest(DigestProcessor),
    Select(SelectProcessor),
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

    let processor = match str_key {
        cmcd::PROCESSOR_CMCD => ProcessorKind::Cmcd(CmcdProcessor::try_from(value)?),
        csv::PROCESSOR_CSV => ProcessorKind::Csv(CsvProcessor::try_from(value)?),
        dissect::PROCESSOR_DISSECT => ProcessorKind::Dissect(DissectProcessor::try_from(value)?),
        epoch::PROCESSOR_EPOCH => ProcessorKind::Epoch(EpochProcessor::try_from(value)?),
        date::PROCESSOR_DATE => ProcessorKind::Date(DateProcessor::try_from(value)?),
        gsub::PROCESSOR_GSUB => ProcessorKind::Gsub(GsubProcessor::try_from(value)?),
        join::PROCESSOR_JOIN => ProcessorKind::Join(JoinProcessor::try_from(value)?),
        letter::PROCESSOR_LETTER => ProcessorKind::Letter(LetterProcessor::try_from(value)?),
        regex::PROCESSOR_REGEX => ProcessorKind::Regex(RegexProcessor::try_from(value)?),
        timestamp::PROCESSOR_TIMESTAMP => {
            ProcessorKind::Timestamp(TimestampProcessor::try_from(value)?)
        }
        urlencoding::PROCESSOR_URL_ENCODING => {
            ProcessorKind::UrlEncoding(UrlEncodingProcessor::try_from(value)?)
        }
        json_path::PROCESSOR_JSON_PATH => {
            ProcessorKind::JsonPath(json_path::JsonPathProcessor::try_from(value)?)
        }
        decolorize::PROCESSOR_DECOLORIZE => {
            ProcessorKind::Decolorize(DecolorizeProcessor::try_from(value)?)
        }
        digest::PROCESSOR_DIGEST => ProcessorKind::Digest(DigestProcessor::try_from(value)?),
        simple_extract::PROCESSOR_SIMPLE_EXTRACT => {
            ProcessorKind::SimpleJsonPath(SimpleExtractProcessor::try_from(value)?)
        }
        json_parse::PROCESSOR_JSON_PARSE => {
            ProcessorKind::JsonParse(JsonParseProcessor::try_from(value)?)
        }
        select::PROCESSOR_SELECT => ProcessorKind::Select(SelectProcessor::try_from(value)?),
        _ => return UnsupportedProcessorSnafu { processor: str_key }.fail(),
    };

    Ok(processor)
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
    yaml_list(v, *STRING_FN, field)
}

pub(crate) fn yaml_list<T>(
    v: &yaml_rust::Yaml,
    conv_fn: impl Fn(&str, &yaml_rust::Yaml) -> Result<T>,
    field: &str,
) -> Result<Vec<T>> {
    v.as_vec()
        .context(FieldMustBeTypeSnafu { field, ty: "list" })?
        .iter()
        .map(|v| conv_fn(field, v))
        .collect()
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
    yaml_list(v, *STRING_OR_HASH_FN, field).map(Fields::new)
}

pub(crate) fn yaml_new_field(v: &yaml_rust::Yaml, field: &str) -> Result<Field> {
    STRING_OR_HASH_FN(field, v)
}
