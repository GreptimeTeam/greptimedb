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

use jsonpath_rust::JsonPath;
use snafu::{OptionExt, ResultExt};
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    Error, JsonParseSnafu, JsonPathParseResultIndexSnafu, JsonPathParseSnafu, KeyMustBeStringSnafu,
    ProcessorMissingFieldSnafu, Result, ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    FIELD_NAME, FIELDS_NAME, IGNORE_MISSING_NAME, JSON_PATH_NAME, JSON_PATH_RESULT_INDEX_NAME,
    Processor, yaml_bool, yaml_new_field, yaml_new_fields, yaml_string,
};

pub(crate) const PROCESSOR_JSON_PATH: &str = "json_path";

impl TryFrom<&yaml_rust::yaml::Hash> for JsonPathProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;
        let mut json_path = None;
        let mut result_idex = None;

        for (k, v) in value.iter() {
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

                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                JSON_PATH_RESULT_INDEX_NAME => {
                    result_idex = Some(v.as_i64().context(JsonPathParseResultIndexSnafu)? as usize);
                }

                JSON_PATH_NAME => {
                    let json_path_str = yaml_string(v, JSON_PATH_NAME)?;
                    json_path = Some(
                        JsonPath::try_from(json_path_str.as_str()).context(JsonPathParseSnafu)?,
                    );
                }

                _ => {}
            }
        }

        let processor = JsonPathProcessor {
            fields,
            json_path: json_path.context(ProcessorMissingFieldSnafu {
                processor: PROCESSOR_JSON_PATH,
                field: JSON_PATH_NAME,
            })?,
            ignore_missing,
            result_index: result_idex,
        };

        Ok(processor)
    }
}

#[derive(Debug)]
pub struct JsonPathProcessor {
    fields: Fields,
    json_path: JsonPath<serde_json::Value>,
    ignore_missing: bool,
    result_index: Option<usize>,
}

impl Default for JsonPathProcessor {
    fn default() -> Self {
        JsonPathProcessor {
            fields: Fields::default(),
            json_path: JsonPath::try_from("$").unwrap(),
            ignore_missing: false,
            result_index: None,
        }
    }
}

impl JsonPathProcessor {
    fn process_field(&self, val: &VrlValue) -> Result<VrlValue> {
        let v = serde_json::to_value(val).context(JsonParseSnafu)?;
        let p = self.json_path.find(&v);
        match p {
            serde_json::Value::Array(arr) => {
                if let Some(index) = self.result_index {
                    Ok(arr
                        .get(index)
                        .cloned()
                        .map(|v| v.into())
                        .unwrap_or(VrlValue::Null))
                } else {
                    Ok(VrlValue::Array(arr.into_iter().map(|v| v.into()).collect()))
                }
            }
            v => Ok(v.into()),
        }
    }
}

impl Processor for JsonPathProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_JSON_PATH
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            match val.get(index) {
                Some(v) => {
                    let processed = self.process_field(v)?;
                    let output_index = field.target_or_input_field();
                    val.insert(KeyString::from(output_index), processed);
                }
                None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
            }
        }
        Ok(val)
    }
}

#[cfg(test)]
mod test {
    use std::collections::BTreeMap;

    use vrl::prelude::Bytes;

    #[test]
    fn test_json_path() {
        use super::*;

        let json_path = JsonPath::try_from("$.hello").unwrap();
        let processor = JsonPathProcessor {
            json_path,
            result_index: Some(0),
            ..Default::default()
        };

        let result = processor
            .process_field(&VrlValue::Object(BTreeMap::from([(
                KeyString::from("hello"),
                VrlValue::Bytes(Bytes::from("world")),
            )])))
            .unwrap();
        assert_eq!(result, VrlValue::Bytes(Bytes::from("world")));
    }
}
