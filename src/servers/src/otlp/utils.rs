use std::collections::BTreeMap;

use itertools::Itertools;
use jsonb::{Number as JsonbNumber, Value as JsonbValue};
use opentelemetry_proto::tonic::common::v1::{any_value, KeyValue};

pub fn bytes_to_hex_string(bs: &[u8]) -> String {
    bs.iter().map(|b| format!("{:02x}", b)).join("")
}

pub fn any_value_to_jsonb(value: any_value::Value) -> JsonbValue<'static> {
    match value {
        any_value::Value::StringValue(s) => JsonbValue::String(s.into()),
        any_value::Value::IntValue(i) => JsonbValue::Number(JsonbNumber::Int64(i)),
        any_value::Value::DoubleValue(d) => JsonbValue::Number(JsonbNumber::Float64(d)),
        any_value::Value::BoolValue(b) => JsonbValue::Bool(b),
        any_value::Value::ArrayValue(a) => {
            let values = a
                .values
                .into_iter()
                .map(|v| match v.value {
                    Some(value) => any_value_to_jsonb(value),
                    None => JsonbValue::Null,
                })
                .collect();
            JsonbValue::Array(values)
        }
        any_value::Value::KvlistValue(kv) => key_value_to_jsonb(kv.values),
        any_value::Value::BytesValue(b) => JsonbValue::String(bytes_to_hex_string(&b).into()),
    }
}

pub fn key_value_to_jsonb(key_values: Vec<KeyValue>) -> JsonbValue<'static> {
    let mut map = BTreeMap::new();
    for kv in key_values {
        let value = match kv.value {
            Some(value) => match value.value {
                Some(value) => any_value_to_jsonb(value),
                None => JsonbValue::Null,
            },
            None => JsonbValue::Null,
        };
        map.insert(kv.key.clone(), value);
    }
    JsonbValue::Object(map)
}
