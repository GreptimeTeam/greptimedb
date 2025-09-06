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
use urlencoding::{decode_binary, encode_binary};
use vrl::prelude::Bytes;
use vrl::value::{KeyString, Value as VrlValue};

use crate::error::{
    Error, KeyMustBeStringSnafu, ProcessorExpectStringSnafu, ProcessorMissingFieldSnafu, Result,
    UrlEncodingInvalidMethodSnafu, ValueMustBeMapSnafu,
};
use crate::etl::field::Fields;
use crate::etl::processor::{
    FIELD_NAME, FIELDS_NAME, IGNORE_MISSING_NAME, METHOD_NAME, yaml_bool, yaml_new_field,
    yaml_new_fields, yaml_string,
};

pub(crate) const PROCESSOR_URL_ENCODING: &str = "urlencoding";

#[derive(Debug, Default)]
enum Method {
    #[default]
    Decode,
    Encode,
}

impl std::fmt::Display for Method {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Method::Decode => write!(f, "decode"),
            Method::Encode => write!(f, "encode"),
        }
    }
}

impl std::str::FromStr for Method {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "decode" => Ok(Method::Decode),
            "encode" => Ok(Method::Encode),
            _ => UrlEncodingInvalidMethodSnafu { s }.fail(),
        }
    }
}

/// only support string value
#[derive(Debug, Default)]
pub struct UrlEncodingProcessor {
    fields: Fields,
    method: Method,
    ignore_missing: bool,
}

impl UrlEncodingProcessor {
    fn process_field(&self, val: &Bytes) -> Result<VrlValue> {
        let processed = match self.method {
            Method::Encode => Bytes::from_iter(encode_binary(val).bytes()),
            Method::Decode => Bytes::from(decode_binary(val).to_vec()),
        };
        Ok(VrlValue::Bytes(processed))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for UrlEncodingProcessor {
    type Error = Error;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self> {
        let mut fields = Fields::default();
        let mut method = Method::Decode;
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

                METHOD_NAME => {
                    let method_str = yaml_string(v, METHOD_NAME)?;
                    method = method_str.parse()?;
                }

                _ => {}
            }
        }
        let processor = UrlEncodingProcessor {
            fields,
            method,
            ignore_missing,
        };

        Ok(processor)
    }
}

impl crate::etl::processor::Processor for UrlEncodingProcessor {
    fn kind(&self) -> &str {
        PROCESSOR_URL_ENCODING
    }

    fn ignore_missing(&self) -> bool {
        self.ignore_missing
    }

    fn exec_mut(&self, mut val: VrlValue) -> Result<VrlValue> {
        for field in self.fields.iter() {
            let index = field.input_field();
            let val = val.as_object_mut().context(ValueMustBeMapSnafu)?;
            match val.get(index) {
                Some(VrlValue::Bytes(s)) => {
                    let result = self.process_field(s)?;
                    let output_index = field.target_or_input_field();
                    val.insert(KeyString::from(output_index), result);
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

    use crate::etl::field::Fields;
    use crate::etl::processor::urlencoding::UrlEncodingProcessor;

    #[test]
    fn test_decode_url() {
        let decoded = "//BC/[a=6.7.8.9,c=g,k=0,l=1]";
        let encoded = "%2F%2FBC%2F%5Ba%3D6.7.8.9%2Cc%3Dg%2Ck%3D0%2Cl%3D1%5D";

        {
            let processor = UrlEncodingProcessor::default();
            let result = processor.process_field(&Bytes::from(encoded)).unwrap();
            assert_eq!(VrlValue::Bytes(Bytes::from(decoded)), result)
        }
        {
            let processor = UrlEncodingProcessor {
                fields: Fields::default(),
                method: super::Method::Encode,
                ignore_missing: false,
            };
            let result = processor.process_field(&Bytes::from(decoded)).unwrap();
            assert_eq!(VrlValue::Bytes(Bytes::from(encoded)), result)
        }
    }
}
