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

use snafu::OptionExt;
use vrl::prelude::Bytes;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    Error, JoinSeparatorRequiredSnafu, KeyMustBeStringSnafu, ProcessorExpectStringSnafu,
    ProcessorMissingFieldSnafu, Result, ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    yaml_bool, yaml_new_field, yaml_new_fields, yaml_string, Processor, FIELDS_NAME, FIELD_NAME,
    IGNORE_MISSING_NAME, SEPARATOR_NAME,
};

pub(crate) const PROCESSOR_JOIN: &str = "join";

/// A processor to join each element of an array into a single string using a separator string between each element
#[derive(Debug, Default)]
pub struct JoinProcessor {
    fields: Fields,
    separator: String,
    ignore_missing: bool,
}

impl JoinProcessor {
    fn process(&self, arr: &[VrlValue]) -> Result<VrlValue> {
        let val = arr
            .iter()
            .map(|v| v.to_string_lossy())
            .collect::<Vec<_>>()
            .join(&self.separator);

        Ok(VrlValue::Bytes(Bytes::from(val)))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for JoinProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut separator = None;
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
                SEPARATOR_NAME => {
                    separator = Some(yaml_string(v, SEPARATOR_NAME)?);
                }
                IGNORE_MISSING_NAME => {
                    ignore_missing = yaml_bool(v, IGNORE_MISSING_NAME)?;
                }
                _ => {}
            }
        }

        Ok(JoinProcessor {
            fields,
            separator: separator.context(JoinSeparatorRequiredSnafu)?,
            ignore_missing,
        })
    }
}

impl Processor for JoinProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_JOIN
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            match val.get(index) {
                Some(VrlValue::Array(arr)) => {
                    let result = self.process(arr)?;
                    let output_index = field.target_or_input_field();
                    val.insert(KeyString::from(output_index.to_string()), result);
                }
                Some(VrlValue::Null) | None => {
                    if !self.ignore_missing {
                        return ProcessorMissingFieldSnafu {
                            processor: self.kind(),
                            field: field.input_field(),
                        }
                        .fail();
                    }
                }
                Some(v) => {
                    return ProcessorExpectStringSnafu {
                        processor: self.kind(),
                        v: v.clone(),
                    }
                    .fail();
                }
            }
        }

        Ok(val)
    }
}

#[cfg(test)]
mod tests {

    use vrl::prelude::Bytes;
    use vrl::value::Value as VrlValue;

    use crate::etl::processor::join::JoinProcessor;

    #[test]
    fn test_join_processor() {
        let processor = JoinProcessor {
            separator: "-".to_string(),
            ..Default::default()
        };

        let arr = vec![
            VrlValue::Bytes(Bytes::from("a")),
            VrlValue::Bytes(Bytes::from("b")),
        ];
        let result = processor.process(&arr).unwrap();
        assert_eq!(result, VrlValue::Bytes(Bytes::from("a-b")));
    }
}
