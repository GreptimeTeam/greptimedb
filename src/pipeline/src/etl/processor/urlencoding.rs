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

use urlencoding::{decode, encode};

use crate::etl::field::{Field, Fields};
use crate::etl::processor::{
    yaml_bool, yaml_field, yaml_fields, yaml_string, FIELDS_NAME, FIELD_NAME, IGNORE_MISSING_NAME,
    METHOD_NAME,
};
use crate::etl::value::{Map, Value};

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
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "decode" => Ok(Method::Decode),
            "encode" => Ok(Method::Encode),
            _ => Err(format!("invalid method: {s}")),
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
    fn with_fields(&mut self, fields: Fields) {
        self.fields = fields;
    }

    fn with_ignore_missing(&mut self, ignore_missing: bool) {
        self.ignore_missing = ignore_missing;
    }

    fn with_method(&mut self, method: Method) {
        self.method = method;
    }

    fn process_field(&self, val: &str, field: &Field) -> Result<Map, String> {
        let processed = match self.method {
            Method::Encode => encode(val).to_string(),
            Method::Decode => decode(val).map_err(|e| e.to_string())?.into_owned(),
        };
        let val = Value::String(processed);

        let key = match field.target_field {
            Some(ref target_field) => target_field,
            None => field.get_field(),
        };

        Ok(Map::one(key, val))
    }
}

impl TryFrom<&yaml_rust::yaml::Hash> for UrlEncodingProcessor {
    type Error = String;

    fn try_from(value: &yaml_rust::yaml::Hash) -> Result<Self, Self::Error> {
        let mut processor = UrlEncodingProcessor::default();

        for (k, v) in value.iter() {
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

                IGNORE_MISSING_NAME => {
                    processor.with_ignore_missing(yaml_bool(v, IGNORE_MISSING_NAME)?);
                }

                METHOD_NAME => {
                    let method = yaml_string(v, METHOD_NAME)?;
                    processor.with_method(method.parse()?);
                }

                _ => {}
            }
        }

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

    fn fields(&self) -> &Fields {
        &self.fields
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
}

#[cfg(test)]
mod tests {
    use crate::etl::field::{Field, Fields};
    use crate::etl::processor::urlencoding::UrlEncodingProcessor;
    use crate::etl::value::{Map, Value};

    #[test]
    fn test_decode_url() {
        let field = "url";
        let ff: Field = field.parse().unwrap();

        let decoded = "//BC/[a=6.7.8.9,c=g,k=0,l=1]";
        let encoded = "%2F%2FBC%2F%5Ba%3D6.7.8.9%2Cc%3Dg%2Ck%3D0%2Cl%3D1%5D";

        let mut processor = UrlEncodingProcessor::default();
        processor.with_fields(Fields::one(ff.clone()));

        {
            let result = processor.process_field(encoded, &ff).unwrap();
            assert_eq!(Map::one(field, Value::String(decoded.into())), result)
        }
        {
            processor.with_method(super::Method::Encode);
            let result = processor.process_field(decoded, &ff).unwrap();
            assert_eq!(Map::one(field, Value::String(encoded.into())), result)
        }
    }
}
