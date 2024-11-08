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

use std::collections::BTreeMap;

use api::v1::value::ValueData;
use api::v1::ColumnDataType;
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

#[inline]
pub(crate) fn make_string_column_data(
    name: &str,
    value: String,
) -> (String, ColumnDataType, ValueData) {
    make_column_data(name, ColumnDataType::String, ValueData::StringValue(value))
}

#[inline]
pub(crate) fn make_column_data(
    name: &str,
    data_type: ColumnDataType,
    value: ValueData,
) -> (String, ColumnDataType, ValueData) {
    (name.to_string(), data_type, value)
}
