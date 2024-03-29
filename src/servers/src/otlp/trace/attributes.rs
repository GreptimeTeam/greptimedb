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

impl OtlpAnyValue<'_> {
    pub fn none() -> Self {
        Self(&AnyValue { value: None })
    }
}

/// specialize Display when it's only a String
impl Display for OtlpAnyValue<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(StringValue(v)) = &self.0.value {
            write!(f, "{v}")
        } else {
            write!(f, "{}", serde_json::to_string(self).unwrap_or_default())
        }
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
                        match &kv.value {
                            Some(val) => map.serialize_entry(&kv.key, &OtlpAnyValue::from(val))?,
                            None => map.serialize_entry(&kv.key, &OtlpAnyValue::none())?,
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
            match &attr.value {
                Some(val) => map.serialize_entry(&attr.key, &OtlpAnyValue::from(val))?,
                None => map.serialize_entry(&attr.key, &OtlpAnyValue::none())?,
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

    use crate::otlp::trace::attributes::{Attributes, OtlpAnyValue};

    #[test]
    fn test_null_value() {
        let otlp_value = OtlpAnyValue::from(&AnyValue { value: None });
        assert_eq!("null", serde_json::to_string(&otlp_value).unwrap())
    }

    #[test]
    fn test_otlp_any_value_display() {
        let values = vec![
            (
                "string value",
                Value::StringValue(String::from("string value")),
            ),
            ("true", Value::BoolValue(true)),
            ("1", Value::IntValue(1)),
            ("1.1", Value::DoubleValue(1.1)),
            ("[1,2,3]", Value::BytesValue(vec![1, 2, 3])),
        ];

        for (expect, val) in values {
            let any_value = AnyValue { value: Some(val) };
            let otlp_value = OtlpAnyValue::from(&any_value);
            assert_eq!(expect, otlp_value.to_string());
        }
    }

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
            assert_eq!(expect, serde_json::to_string(&otlp_value).unwrap());
        }
    }

    #[test]
    fn test_any_value_array_type_serialize() {
        let values = vec![
            ("[]", vec![]),
            ("[null]", vec![AnyValue { value: None }]),
            (
                r#"["string1","string2","string3"]"#,
                vec![
                    AnyValue {
                        value: Some(Value::StringValue(String::from("string1"))),
                    },
                    AnyValue {
                        value: Some(Value::StringValue(String::from("string2"))),
                    },
                    AnyValue {
                        value: Some(Value::StringValue(String::from("string3"))),
                    },
                ],
            ),
            (
                "[1,2,3]",
                vec![
                    AnyValue {
                        value: Some(Value::IntValue(1)),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(2)),
                    },
                    AnyValue {
                        value: Some(Value::IntValue(3)),
                    },
                ],
            ),
            (
                "[1.1,2.2,3.3]",
                vec![
                    AnyValue {
                        value: Some(Value::DoubleValue(1.1)),
                    },
                    AnyValue {
                        value: Some(Value::DoubleValue(2.2)),
                    },
                    AnyValue {
                        value: Some(Value::DoubleValue(3.3)),
                    },
                ],
            ),
            (
                "[true,false,true]",
                vec![
                    AnyValue {
                        value: Some(Value::BoolValue(true)),
                    },
                    AnyValue {
                        value: Some(Value::BoolValue(false)),
                    },
                    AnyValue {
                        value: Some(Value::BoolValue(true)),
                    },
                ],
            ),
            (
                r#"[1,1.1,"str_value",true,null]"#,
                vec![
                    AnyValue {
                        value: Some(Value::IntValue(1)),
                    },
                    AnyValue {
                        value: Some(Value::DoubleValue(1.1)),
                    },
                    AnyValue {
                        value: Some(Value::StringValue("str_value".into())),
                    },
                    AnyValue {
                        value: Some(Value::BoolValue(true)),
                    },
                    AnyValue { value: None },
                ],
            ),
        ];

        for (expect, values) in values {
            let any_val = AnyValue {
                value: Some(Value::ArrayValue(ArrayValue { values })),
            };
            let otlp_value = OtlpAnyValue::from(&any_val);
            assert_eq!(expect, serde_json::to_string(&otlp_value).unwrap());
        }
    }

    #[test]
    fn test_any_value_map_type_serialize() {
        let cases = vec![
            ("{}", vec![]),
            (
                r#"{"key1":null}"#,
                vec![KeyValue {
                    key: "key1".into(),
                    value: None,
                }],
            ),
            (
                r#"{"key1":null}"#,
                vec![KeyValue {
                    key: "key1".into(),
                    value: Some(AnyValue { value: None }),
                }],
            ),
            (
                r#"{"key1":"val1"}"#,
                vec![KeyValue {
                    key: "key1".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(String::from("val1"))),
                    }),
                }],
            ),
        ];

        for (expect, values) in cases {
            let any_val = AnyValue {
                value: Some(Value::KvlistValue(KeyValueList { values })),
            };
            let otlp_value = OtlpAnyValue::from(&any_val);
            assert_eq!(expect, serde_json::to_string(&otlp_value).unwrap());
        }
    }

    #[test]
    fn test_attributes_serialize() {
        let cases = vec![
            ("{}", vec![]),
            (
                r#"{"key1":null}"#,
                vec![KeyValue {
                    key: "key1".into(),
                    value: None,
                }],
            ),
            (
                r#"{"key1":null}"#,
                vec![KeyValue {
                    key: "key1".into(),
                    value: Some(AnyValue { value: None }),
                }],
            ),
            (
                r#"{"key1":"val1"}"#,
                vec![KeyValue {
                    key: "key1".into(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(String::from("val1"))),
                    }),
                }],
            ),
        ];

        for (expect, values) in cases {
            assert_eq!(expect, serde_json::to_string(&Attributes(values)).unwrap());
        }
    }
}
