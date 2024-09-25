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

// Reference: https://www.elastic.co/guide/en/elasticsearch/reference/current/csv-processor.html

use ahash::HashSet;
use csv::{ReaderBuilder, Trim};
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};

use crate::etl::error::{
    CsvNoRecordSnafu, CsvQuoteNameSnafu, CsvReadSnafu, CsvSeparatorNameSnafu, Error,
    KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::{Fields, InputFieldInfo, OneInputMultiOutputField};
use crate::etl::find_key_index;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, ProcessorBuilder,
    ProcessorKind, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::etl::value::Value;

pub(crate) const PROCESSOR_CSV: &str = "csv";

const SEPARATOR_NAME: &str = "separator";
const QUOTE_NAME: &str = "quote";
const TRIM_NAME: &str = "trim";
const EMPTY_VALUE_NAME: &str = "empty_value";
const TARGET_FIELDS: &str = "target_fields";

#[derive(Debug, Default)]
pub struct CsvProcessorBuilder {
    reader: ReaderBuilder,

    fields: Fields,
    ignore_missing: bool,

    // Value used to fill empty fields, empty fields will be skipped if this is not provided.
    empty_value: Option<String>,
    target_fields: Vec<String>,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl CsvProcessorBuilder {
    fn build(self, intermediate_keys: &[String]) -> Result<CsvProcessor> {
        let mut real_fields = vec![];

        for field in self.fields {
            let input_index = find_key_index(intermediate_keys, field.input_field(), "csv")?;

            let input_field_info = InputFieldInfo::new(field.input_field(), input_index);
            let real_field = OneInputMultiOutputField::new(input_field_info, None);
            real_fields.push(real_field);
        }

        let output_index_info = self
            .target_fields
            .iter()
            .map(|f| find_key_index(intermediate_keys, f, "csv"))
            .collect::<Result<Vec<_>>>()?;
        Ok(CsvProcessor {
            reader: self.reader,
            fields: real_fields,
            ignore_missing: self.ignore_missing,
            empty_value: self.empty_value,
            output_index_info,
        })
    }
}

impl ProcessorBuilder for CsvProcessorBuilder {
    fn output_keys(&self) -> HashSet<&str> {
        self.target_fields.iter().map(|s| s.as_str()).collect()
    }

    fn input_keys(&self) -> HashSet<&str> {
        self.fields.iter().map(|f| f.input_field()).collect()
    }

    fn build(self, intermediate_keys: &[String]) -> Result<ProcessorKind> {
        self.build(intermediate_keys).map(ProcessorKind::Csv)
    }
}

/// only support string value
#[derive(Debug)]
pub struct CsvProcessor {
    reader: ReaderBuilder,

    fields: Vec<OneInputMultiOutputField>,

    ignore_missing: bool,

    // Value used to fill empty fields, empty fields will be skipped if this is not provided.
    empty_value: Option<String>,
    output_index_info: Vec<usize>,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl CsvProcessor {
    // process the csv format string to a map with target_fields as keys
    fn process(&self, val: &str) -> Result<Vec<(usize, Value)>> {
        let mut reader = self.reader.from_reader(val.as_bytes());

        if let Some(result) = reader.records().next() {
            let record: csv::StringRecord = result.context(CsvReadSnafu)?;

            let values: Vec<(usize, Value)> = self
                .output_index_info
                .iter()
                .zip_longest(record.iter())
                .filter_map(|zipped| match zipped {
                    Both(target_field, val) => Some((*target_field, Value::String(val.into()))),
                    // if target fields are more than extracted fields, fill the rest with empty value
                    Left(target_field) => {
                        let value = self
                            .empty_value
                            .as_ref()
                            .map(|s| Value::String(s.clone()))
                            .unwrap_or(Value::Null);
                        Some((*target_field, value))
                    }
                    // if extracted fields are more than target fields, ignore the rest
                    Right(_) => None,
                })
                .collect();

            Ok(values)
        } else {
            CsvNoRecordSnafu.fail()
        }
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for CsvProcessorBuilder {
    type Error = Error;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut reader = ReaderBuilder::new();
        reader.has_headers(false);

        let mut fields = Fields::default();
        let mut ignore_missing = false;
        let mut empty_value = None;
        let mut target_fields = vec![];

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
                TARGET_FIELDS => {
                    target_fields = yaml_string(v, TARGET_FIELDS)?
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                }
                SEPARATOR_NAME => {
                    let separator = yaml_string(v, SEPARATOR_NAME)?;
                    if separator.len() != 1 {
                        return CsvSeparatorNameSnafu {
                            separator: SEPARATOR_NAME,
                            value: separator,
                        }
                        .fail();
                    } else {
                        reader.delimiter(separator.as_bytes()[0]);
                    }
                }
                QUOTE_NAME => {
                    let quote = yaml_string(v, QUOTE_NAME)?;
                    if quote.len() != 1 {
                        return CsvQuoteNameSnafu {
                            quote: QUOTE_NAME,
                            value: quote,
                        }
                        .fail();
                    } else {
                        reader.quote(quote.as_bytes()[0]);
                    }
                }
                TRIM_NAME => {
                    let trim = yaml_bool(v, TRIM_NAME)?;
                    if trim {
                        reader.trim(Trim::All);
                    } else {
                        reader.trim(Trim::None);
                    }
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                EMPTY_VALUE_NAME => {
                    empty_value = Some(yaml_string(v, EMPTY_VALUE_NAME)?);
                }

                _ => {}
            }
        }
        let builder = {
            CsvProcessorBuilder {
                reader,
                fields,
                ignore_missing,
                empty_value,
                target_fields,
            }
        };

        Ok(builder)
    }
}

impl Processor for CsvProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_CSV
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<()> {
        for field in self.fields.iter() {
            let index = field.input_index();
            match val.get(index) {
                Some(Value::String(v)) => {
                    let resule_list = self.process(v)?;
                    for (k, v) in resule_list {
                        val[k] = v;
                    }
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

#[cfg(test)]
mod tests {

    use ahash::HashMap;

    use super::Value;
    use crate::etl::processor::csv::CsvProcessorBuilder;

    #[test]
    fn test_equal_length() {
        let mut reader = csv::ReaderBuilder::new();
        reader.has_headers(false);
        let builder = CsvProcessorBuilder {
            reader,
            target_fields: vec!["a".into(), "b".into()],
            ..Default::default()
        };

        let intermediate_keys = vec!["data".into(), "a".into(), "b".into()];

        let processor = builder.build(&intermediate_keys).unwrap();
        let result = processor
            .process("1,2")
            .unwrap()
            .into_iter()
            .map(|(k, v)| (intermediate_keys[k].clone(), v))
            .collect::<HashMap<_, _>>();

        let values = [
            ("a".into(), Value::String("1".into())),
            ("b".into(), Value::String("2".into())),
        ]
        .into_iter()
        .collect::<HashMap<_, _>>();

        assert_eq!(result, values);
    }

    // test target_fields length larger than the record length
    #[test]
    fn test_target_fields_has_more_length() {
        // with no empty value
        {
            let mut reader = csv::ReaderBuilder::new();
            reader.has_headers(false);
            let builder = CsvProcessorBuilder {
                reader,
                target_fields: vec!["a".into(), "b".into(), "c".into()],
                ..Default::default()
            };

            let intermediate_keys = vec!["data".into(), "a".into(), "b".into(), "c".into()];

            let processor = builder.build(&intermediate_keys).unwrap();
            let result = processor
                .process("1,2")
                .unwrap()
                .into_iter()
                .map(|(k, v)| (intermediate_keys[k].clone(), v))
                .collect::<HashMap<_, _>>();

            let values = [
                ("a".into(), Value::String("1".into())),
                ("b".into(), Value::String("2".into())),
                ("c".into(), Value::Null),
            ]
            .into_iter()
            .collect::<HashMap<_, _>>();

            assert_eq!(result, values);
        }

        // with empty value
        {
            let mut reader = csv::ReaderBuilder::new();
            reader.has_headers(false);
            let builder = CsvProcessorBuilder {
                reader,
                target_fields: vec!["a".into(), "b".into(), "c".into()],
                empty_value: Some("default".into()),
                ..Default::default()
            };

            let intermediate_keys = vec!["data".into(), "a".into(), "b".into(), "c".into()];

            let processor = builder.build(&intermediate_keys).unwrap();
            let result = processor
                .process("1,2")
                .unwrap()
                .into_iter()
                .map(|(k, v)| (intermediate_keys[k].clone(), v))
                .collect::<HashMap<_, _>>();

            let values = [
                ("a".into(), Value::String("1".into())),
                ("b".into(), Value::String("2".into())),
                ("c".into(), Value::String("default".into())),
            ]
            .into_iter()
            .collect();

            assert_eq!(result, values);
        }
    }

    // test record has larger length
    #[test]
    fn test_target_fields_has_less_length() {
        let mut reader = csv::ReaderBuilder::new();
        reader.has_headers(false);
        let builder = CsvProcessorBuilder {
            reader,
            target_fields: vec!["a".into(), "b".into()],
            empty_value: Some("default".into()),
            ..Default::default()
        };

        let intermediate_keys = vec!["data".into(), "a".into(), "b".into()];

        let processor = builder.build(&intermediate_keys).unwrap();
        let result = processor
            .process("1,2")
            .unwrap()
            .into_iter()
            .map(|(k, v)| (intermediate_keys[k].clone(), v))
            .collect::<HashMap<_, _>>();

        let values = [
            ("a".into(), Value::String("1".into())),
            ("b".into(), Value::String("2".into())),
        ]
        .into_iter()
        .collect();

        assert_eq!(result, values);
    }
}
