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

use snafu::{OptionExt as _, ResultExt};
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    Error, FieldMustBeTypeSnafu, JsonParseSnafu, KeyMustBeStringSnafu, ProcessorMissingFieldSnafu,
    ProcessorUnsupportedValueSnafu, Result, ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
};
use crate::Processor;

pub(crate) const PROCESSOR_JSON_PARSE: &str = "json_parse";

#[derive(Debug, Default)]
pub struct JsonParseProcessor {
    fields: Fields,
    ignore_missing: bool,
}

impl TryFrom<&yaml_rust::yaml::Hash> for JsonParseProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> std::result::Result<Self, Self::Error> {
        let mut fields = Fields::default();
        let mut ignore_missing = false;

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
                _ => {}
            }
        }

        let processor = JsonParseProcessor {
            fields,
            ignore_missing,
        };

        Ok(processor)
    }
}

impl JsonParseProcessor {
    fn process_field(&self, val: &VrlValue) -> Result<VrlValue> {
        let Some(json_str) = val.as_str() else {
            return FieldMustBeTypeSnafu {
                field: val.to_string(),
                ty: "string",
            }
            .fail();
        };
        let parsed: VrlValue = serde_json::from_str(&json_str).context(JsonParseSnafu)?;
        match parsed {
            VrlValue::Object(_) => Ok(parsed),
            VrlValue::Array(_) => Ok(parsed),
            _ => ProcessorUnsupportedValueSnafu {
                processor: self.kind(),
                val: val.to_string(),
            }
            .fail(),
        }
    }
}

impl Processor for JsonParseProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_JSON_PARSE
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
                    val.insert(KeyString::from(output_index.to_string()), processed);
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
    use vrl::value::{KeyString, Value as VrlValue};

    use crate::etl::processor::json_parse::JsonParseProcessor;

    #[test]
    fn test_json_parse() {
        let processor = JsonParseProcessor {
            ..Default::default()
        };

        let result = processor
            .process_field(&VrlValue::Bytes(Bytes::from(r#"{"hello": "world"}"#)))
            .unwrap();

        let expected = VrlValue::Object(BTreeMap::from([(
            KeyString::from("hello"),
            VrlValue::Bytes(Bytes::from("world")),
        )]));

        assert_eq!(result, expected);
    }
}
