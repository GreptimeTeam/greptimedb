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

use std::fmt::Display;

use opentelemetry_proto::tonic::common::v1::any_value::Value::{
    ArrayValue, BoolValue, BytesValue, DoubleValue, IntValue, KvlistValue, StringValue,
};
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use serde::ser::{SerializeMap, SerializeSeq};
use serde::Serialize;

#[derive(Clone, Debug)]
pub struct OtlpAnyValue<'a>(&'a AnyValue);

impl<'a> From<&'a AnyValue> for OtlpAnyValue<'a> {
    fn from(any_val: &'a AnyValue) -> Self {
        Self(any_val)
    }
}

impl Display for OtlpAnyValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

impl Serialize for OtlpAnyValue<'_> {
    fn serialize<S>(&self, zer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match &self.0.value {
            Some(val) => match &val {
                StringValue(v) => zer.serialize_str(v),
                BoolValue(v) => zer.serialize_bool(*v),
                IntValue(v) => zer.serialize_i64(*v),
                DoubleValue(v) => zer.serialize_f64(*v),
                ArrayValue(v) => {
                    let mut seq = zer.serialize_seq(Some(v.values.len()))?;
                    for val in &v.values {
                        seq.serialize_element(&OtlpAnyValue::from(val))?;
                    }
                    seq.end()
                }
                KvlistValue(v) => {
                    let mut map = zer.serialize_map(Some(v.values.len()))?;
                    for kv in &v.values {
                        if let Some(val) = &kv.value {
                            map.serialize_entry(&kv.key, &OtlpAnyValue::from(val))?;
                        }
                    }
                    map.end()
                }
                BytesValue(v) => zer.serialize_bytes(v),
            },
            None => zer.serialize_none(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Attributes(Vec<KeyValue>);

impl Display for Attributes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
    }
}

impl Serialize for Attributes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for attr in &self.0 {
            if let Some(val) = &attr.value {
                map.serialize_entry(&attr.key, &OtlpAnyValue::from(val))?;
            }
        }
        map.end()
    }
}

impl From<Vec<KeyValue>> for Attributes {
    fn from(attrs: Vec<KeyValue>) -> Self {
        Self(attrs)
    }
}

impl Attributes {
    pub fn get_ref(&self) -> &Vec<KeyValue> {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut Vec<KeyValue> {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValue, KeyValueList};

    use crate::otlp::trace::attributes::OtlpAnyValue;

    #[test]
    fn test_any_value_primitive_type_serialize() {
        let values = vec![
            (
                r#""string value""#,
                Value::StringValue(String::from("string value")),
            ),
            ("true", Value::BoolValue(true)),
            ("1", Value::IntValue(1)),
            ("1.1", Value::DoubleValue(1.1)),
            ("[1,2,3]", Value::BytesValue(vec![1, 2, 3])),
        ];

        for (expect, val) in values {
            let any_val = AnyValue { value: Some(val) };
            let otlp_value = OtlpAnyValue::from(&any_val);
            assert_eq!(
                expect,
                serde_json::to_string(&otlp_value).unwrap_or_default()
            );
        }
    }

    #[test]
    fn test_any_value_array_type_serialize() {
        let values = vec![
            ("[]", vec![]),
            (
                r#"["string1","string2","string3"]"#,
                vec![
                    Value::StringValue(String::from("string1")),
                    Value::StringValue(String::from("string2")),
                    Value::StringValue(String::from("string3")),
                ],
            ),
            (
                "[1,2,3]",
                vec![Value::IntValue(1), Value::IntValue(2), Value::IntValue(3)],
            ),
            (
                "[1.1,2.2,3.3]",
                vec![
                    Value::DoubleValue(1.1),
                    Value::DoubleValue(2.2),
                    Value::DoubleValue(3.3),
                ],
            ),
            (
                "[true,false,true]",
                vec![
                    Value::BoolValue(true),
                    Value::BoolValue(false),
                    Value::BoolValue(true),
                ],
            ),
        ];

        for (expect, vals) in values {
            let array_values: Vec<AnyValue> = vals
                .into_iter()
                .map(|val| AnyValue { value: Some(val) })
                .collect();

            let value = Value::ArrayValue(ArrayValue {
                values: array_values,
            });

            let any_val = AnyValue { value: Some(value) };
            let otlp_value = OtlpAnyValue::from(&any_val);
            assert_eq!(
                expect,
                serde_json::to_string(&otlp_value).unwrap_or_default()
            );
        }
    }

    #[test]
    fn test_any_value_map_type_serialize() {
        let cases = vec![
            ("{}", vec![]),
            (
                r#"{"key1":"val1"}"#,
                vec![("key1", Value::StringValue(String::from("val1")))],
            ),
        ];

        for (expect, kv) in cases {
            let kvlist: Vec<KeyValue> = kv
                .into_iter()
                .map(|(k, v)| KeyValue {
                    key: k.into(),
                    value: Some(AnyValue { value: Some(v) }),
                })
                .collect();

            let value = Value::KvlistValue(KeyValueList { values: kvlist });

            let any_val = AnyValue { value: Some(value) };
            let otlp_value = OtlpAnyValue::from(&any_val);
            assert_eq!(
                expect,
                serde_json::to_string(&otlp_value).unwrap_or_default()
            );
        }
    }
}
