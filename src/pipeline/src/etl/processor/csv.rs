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

use std::collections::BTreeMap;

use csv::{ReaderBuilder, Trim};
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;
use snafu::{OptionExt, ResultExt};

use crate::etl::error::{
    CsvNoRecordSnafu, CsvQuoteNameSnafu, CsvReadSnafu, CsvSeparatorNameSnafu, Error,
    KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME,
};
use crate::etl::value::Value;

pub(crate) const PROCESSOR_CSV: &str = "csv";

const SEPARATOR_NAME: &str = "separator";
const QUOTE_NAME: &str = "quote";
const TRIM_NAME: &str = "trim";
const EMPTY_VALUE_NAME: &str = "empty_value";
const TARGET_FIELDS: &str = "target_fields";

/// only support string value
#[derive(Debug, Default)]
pub struct CsvProcessor {
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

impl CsvProcessor {
    // process the csv format string to a map with target_fields as keys
    fn process(&self, val: &str) -> Result<BTreeMap<String, Value>> {
        let mut reader = self.reader.from_reader(val.as_bytes());

        if let Some(result) = reader.records().next() {
            let record: csv::StringRecord = result.context(CsvReadSnafu)?;

            let values = self
                .target_fields
                .iter()
                .zip_longest(record.iter())
                .filter_map(|zipped| match zipped {
                    Both(target_field, val) => {
                        Some((target_field.clone(), Value::String(val.into())))
                    }
                    // if target fields are more than extracted fields, fill the rest with empty value
                    Left(target_field) => {
                        let value = self
                            .empty_value
                            .as_ref()
                            .map(|s| Value::String(s.clone()))
                            .unwrap_or(Value::Null);
                        Some((target_field.clone(), value))
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

impl TryFrom<&yaml_rust::yaml::Hash> for CsvProcessor {
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
        let proc = {
            CsvProcessor {
                reader,
                fields,
                ignore_missing,
                empty_value,
                target_fields,
            }
        };

        Ok(proc)
    }
}

impl Processor for CsvProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_CSV
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, val: &mut BTreeMap<String, Value>) -> Result<()> {
        for field in self.fields.iter() {
            let name = field.input_field();

            match val.get(name) {
                Some(Value::String(v)) => {
                    let results = self.process(v)?;
                    val.extend(results);
                }
                Some(Value::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind().to_string(),
                            field: name.to_string(),
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

    use super::*;
    use crate::etl::field::Field;

    #[test]
    fn test_equal_length() {
        let mut reader = csv::ReaderBuilder::new();
        reader.has_headers(false);
        let processor = CsvProcessor {
            reader,
            fields: Fields::new(vec![Field::new("data", None)]),
            target_fields: vec!["a".into(), "b".into()],
            ..Default::default()
        };

        let result = processor.process("1,2").unwrap();

        let values = [
            ("a".into(), Value::String("1".into())),
            ("b".into(), Value::String("2".into())),
        ]
        .into_iter()
        .collect();

        assert_eq!(result, values);
    }

    // test target_fields length larger than the record length
    #[test]
    fn test_target_fields_has_more_length() {
        // with no empty value
        {
            let mut reader = csv::ReaderBuilder::new();
            reader.has_headers(false);
            let processor = CsvProcessor {
                reader,
                fields: Fields::new(vec![Field::new("data", None)]),
                target_fields: vec!["a".into(), "b".into(), "c".into()],
                ..Default::default()
            };

            let result = processor.process("1,2").unwrap();

            let values = [
                ("a".into(), Value::String("1".into())),
                ("b".into(), Value::String("2".into())),
                ("c".into(), Value::Null),
            ]
            .into_iter()
            .collect();

            assert_eq!(result, values);
        }

        // with empty value
        {
            let mut reader = csv::ReaderBuilder::new();
            reader.has_headers(false);
            let processor = CsvProcessor {
                reader,
                fields: Fields::new(vec![Field::new("data", None)]),
                target_fields: vec!["a".into(), "b".into(), "c".into()],
                empty_value: Some("default".into()),
                ..Default::default()
            };

            let result = processor.process("1,2").unwrap();

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
        let processor = CsvProcessor {
            reader,
            target_fields: vec!["a".into(), "b".into()],
            empty_value: Some("default".into()),
            ..Default::default()
        };

        let result = processor.process("1,2").unwrap();

        let values = [
            ("a".into(), Value::String("1".into())),
            ("b".into(), Value::String("2".into())),
        ]
        .into_iter()
        .collect();

        assert_eq!(result, values);
    }
}
