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

use ahash::{HashMap, HashSet};
use csv::{ReaderBuilder, Trim};
use itertools::EitherOrBoth::{Both, Left, Right};
use itertools::Itertools;

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, yaml_string, Processor, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME,
};
use crate::etl::value::{Map, Value};

pub(crate) const PROCESSOR_CSV: &str = "csv";

const SEPARATOR_NAME: &str = "separator";
const QUOTE_NAME: &str = "quote";
const TRIM_NAME: &str = "trim";
const EMPTY_VALUE_NAME: &str = "empty_value";

/// only support string value
#[derive(Debug)]
pub struct CsvProcessor {
    reader: ReaderBuilder,

    fields: Fields,

    ignore_missing: bool,

    // Value used to fill empty fields, empty fields will be skipped if this is not provided.
    empty_value: Option<String>,
    // description
    // if
    // ignore_failure
    // on_failure
    // tag
}

impl CsvProcessor {
    fn new() -> Self {
        let mut reader = ReaderBuilder::new();
        reader.has_headers(false);

        Self {
            reader,
            fields: Fields::default(),
            ignore_missing: false,
            empty_value: None,
        }
    }

    fn with_fields(&mut self, fields: Fields) {
        self.fields = fields;
    }

    fn try_separator(&mut self, separator: String) -> Result<(), String> {
        if separator.len() != 1 {
            Err(format!(
                "'{}' must be a single character, but got '{}'",
                SEPARATOR_NAME, separator
            ))
        } else {
            self.reader.delimiter(separator.as_bytes()[0]);
            Ok(())
        }
    }

    fn try_quote(&mut self, quote: String) -> Result<(), String> {
        if quote.len() != 1 {
            Err(format!(
                "'{}' must be a single character, but got '{}'",
                QUOTE_NAME, quote
            ))
        } else {
            self.reader.quote(quote.as_bytes()[0]);
            Ok(())
        }
    }

    fn with_trim(&mut self, trim: bool) {
        if trim {
            self.reader.trim(Trim::All);
        } else {
            self.reader.trim(Trim::None);
        }
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn with_empty_value(&mut self, empty_value: String) {
        self.empty_value = Some(empty_value);
    }

    // process the csv format string to a map with target_fields as keys
    fn process_field(&self, val: &str, field: &Field) -> Result<Map, String> {
        let mut reader = self.reader.from_reader(val.as_bytes());

        if let Some(result) = reader.records().next() {
            let record: csv::StringRecord = result.map_err(|e| e.to_string())?;

            let values: HashMap<String, Value> = field
                .target_fields
                .as_ref()
                .ok_or(format!(
                    "target fields must be set after '{}'",
                    field.get_field_name()
                ))?
                .iter()
                .map(|f| f.to_string())
                .zip_longest(record.iter())
                .filter_map(|zipped| match zipped {
                    Both(target_field, val) => Some((target_field, Value::String(val.into()))),
                    // if target fields are more than extracted fields, fill the rest with empty value
                    Left(target_field) => {
                        let value = self
                            .empty_value
                            .as_ref()
                            .map(|s| Value::String(s.clone()))
                            .unwrap_or(Value::Null);
                        Some((target_field, value))
                    }
                    // if extracted fields are more than target fields, ignore the rest
                    Right(_) => None,
                })
                .collect();

            Ok(Map { values })
        } else {
            Err("expected at least one record from csv format, but got none".into())
        }
    }

    fn update_output_keys(&mut self) {
        self.fields.iter_mut().for_each(|f| {
            if let Some(tfs) = f.target_fields.as_ref() {
                tfs.iter().for_each(|tf| {
                    if !tf.is_empty() {
                        f.output_fields.insert(tf.to_string(), 0);
                    }
                });
            }
        })
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for CsvProcessor {
    type Error = String;

    fn try_from(hash: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = CsvProcessor::new();
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
                SEPARATOR_NAME => {
                    processor.try_separator(yaml_string(v, SEPARATOR_NAME)?)?;
                }
                QUOTE_NAME => {
                    processor.try_quote(yaml_string(v, QUOTE_NAME)?)?;
                }
                TRIM_NAME => {
                    processor.with_trim(yaml_bool(v, TRIM_NAME)?);
                }
                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }
                EMPTY_VALUE_NAME => {
                    processor.with_empty_value(yaml_string(v, EMPTY_VALUE_NAME)?);
                }

                _ => {}
            }
        }
        processor.update_output_keys();
        Ok(processor)
    }
}

impl Processor for CsvProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_CSV
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
            .flat_map(|f| f.target_fields.clone().unwrap_or_default())
            .collect()
    }

    fn exec_field(&self, val: &Value, field: &Field) -> Result<Map, String> {
        match val {
            Value::String(val) => self.process_field(val, field),
            _ => Err(format!(
                "{} processor: expect string value, but got {val:?}",
                self.kind()
            )),
        }
    }

    fn exec_mut(&self, val: &mut Vec<Value>) -> Result<(), String> {
        for field in self.fields.iter() {
            match val.get(field.input_field.index) {
                Some(Value::String(v)) => {
                    // TODO(qtang): Let this method use the intermediate state collection directly.
                    let map = self.process_field(v, field)?;
                    for (k, v) in map.values.into_iter() {
                        if let Some(index) = field.output_fields.get(&k) {
                            val[*index] = v;
                        }
                    }
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
}

// TODO(yuanbohan): more test cases
#[cfg(test)]
mod tests {
    use ahash::HashMap;

    use super::{CsvProcessor, Value};
    use crate::etl::field::Fields;
    use crate::etl::processor::Processor;
    use crate::etl::value::Map;

    #[test]
    fn test_equal_length() {
        let mut processor = CsvProcessor::new();
        let field = "data,, a, b".parse().unwrap();
        processor.with_fields(Fields::one(field));

        let values: HashMap<String, Value> = [("data".into(), Value::String("1,2".into()))]
            .into_iter()
            .collect();
        let mut m = Map { values };

        let result = processor.exec_map(&mut m).unwrap();

        let values = [
            ("data".into(), Value::String("1,2".into())),
            ("a".into(), Value::String("1".into())),
            ("b".into(), Value::String("2".into())),
        ]
        .into_iter()
        .collect();
        let mut expected = Map { values };

        assert_eq!(&mut expected, result);
    }

    // test target_fields length larger than the record length
    #[test]
    fn test_target_fields_has_more_length() {
        let values = [("data".into(), Value::String("1,2".into()))]
            .into_iter()
            .collect();
        let mut input = Map { values };

        // with no empty value
        {
            let mut processor = CsvProcessor::new();
            let field = "data,, a,b,c".parse().unwrap();
            processor.with_fields(Fields::one(field));

            let result = processor.exec_map(&mut input).unwrap();

            let values = [
                ("data".into(), Value::String("1,2".into())),
                ("a".into(), Value::String("1".into())),
                ("b".into(), Value::String("2".into())),
                ("c".into(), Value::Null),
            ]
            .into_iter()
            .collect();
            let mut expected = Map { values };

            assert_eq!(&mut expected, result);
        }

        // with empty value
        {
            let mut processor = CsvProcessor::new();
            let field = "data,, a,b,c".parse().unwrap();
            processor.with_fields(Fields::one(field));
            processor.with_empty_value("default".into());

            let result = processor.exec_map(&mut input).unwrap();

            let values = [
                ("data".into(), Value::String("1,2".into())),
                ("a".into(), Value::String("1".into())),
                ("b".into(), Value::String("2".into())),
                ("c".into(), Value::String("default".into())),
            ]
            .into_iter()
            .collect();
            let mut expected = Map { values };

            assert_eq!(&mut expected, result);
        }
    }

    // test record has larger length
    #[test]
    fn test_target_fields_has_less_length() {
        let values = [("data".into(), Value::String("1,2,3".into()))]
            .into_iter()
            .collect();
        let mut input = Map { values };

        let mut processor = CsvProcessor::new();
        let field = "data,,a,b".parse().unwrap();
        processor.with_fields(Fields::one(field));

        let result = processor.exec_map(&mut input).unwrap();

        let values = [
            ("data".into(), Value::String("1,2,3".into())),
            ("a".into(), Value::String("1".into())),
            ("b".into(), Value::String("2".into())),
        ]
        .into_iter()
        .collect();
        let mut expected = Map { values };

        assert_eq!(&mut expected, result);
    }
}
