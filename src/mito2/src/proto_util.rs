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

//! Utilities to process protobuf messages.

use common_time::timestamp::TimeUnit;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::{TimeType, TimestampType};
use datatypes::value::Value;
use greptime_proto::v1::{self, ColumnDataType};

use crate::metadata::SemanticType;

/// Returns true if the pb semantic type is valid.
pub(crate) fn check_semantic_type(type_value: i32, semantic_type: SemanticType) -> bool {
    type_value == semantic_type as i32
}

/// Returns true if the pb type value is valid.
pub(crate) fn check_column_type(type_value: i32, expect_type: &ConcreteDataType) -> bool {
    let Some(column_type) = ColumnDataType::from_i32(type_value) else {
        return false;
    };

    is_column_type_eq(column_type, expect_type)
}

/// Convert value into proto's value.
pub(crate) fn to_proto_value(value: Value) -> Option<v1::Value> {
    let proto_value = match value {
        Value::Null => v1::Value { value: None },
        Value::Boolean(v) => v1::Value {
            value: Some(v1::value::Value::BoolValue(v)),
        },
        Value::UInt8(v) => v1::Value {
            value: Some(v1::value::Value::U8Value(v.into())),
        },
        Value::UInt16(v) => v1::Value {
            value: Some(v1::value::Value::U16Value(v.into())),
        },
        Value::UInt32(v) => v1::Value {
            value: Some(v1::value::Value::U32Value(v)),
        },
        Value::UInt64(v) => v1::Value {
            value: Some(v1::value::Value::U64Value(v)),
        },
        Value::Int8(v) => v1::Value {
            value: Some(v1::value::Value::I8Value(v.into())),
        },
        Value::Int16(v) => v1::Value {
            value: Some(v1::value::Value::I16Value(v.into())),
        },
        Value::Int32(v) => v1::Value {
            value: Some(v1::value::Value::I32Value(v)),
        },
        Value::Int64(v) => v1::Value {
            value: Some(v1::value::Value::I64Value(v)),
        },
        Value::Float32(v) => v1::Value {
            value: Some(v1::value::Value::F32Value(*v)),
        },
        Value::Float64(v) => v1::Value {
            value: Some(v1::value::Value::F64Value(*v)),
        },
        Value::String(v) => v1::Value {
            value: Some(v1::value::Value::StringValue(v.as_utf8().to_string())),
        },
        Value::Binary(v) => v1::Value {
            value: Some(v1::value::Value::BinaryValue(v.to_vec())),
        },
        Value::Date(v) => v1::Value {
            value: Some(v1::value::Value::DateValue(v.val())),
        },
        Value::DateTime(v) => v1::Value {
            value: Some(v1::value::Value::DatetimeValue(v.val())),
        },
        Value::Timestamp(v) => match v.unit() {
            TimeUnit::Second => v1::Value {
                value: Some(v1::value::Value::TsSecondValue(v.value())),
            },
            TimeUnit::Millisecond => v1::Value {
                value: Some(v1::value::Value::TsMillisecondValue(v.value())),
            },
            TimeUnit::Microsecond => v1::Value {
                value: Some(v1::value::Value::TsMicrosecondValue(v.value())),
            },
            TimeUnit::Nanosecond => v1::Value {
                value: Some(v1::value::Value::TsNanosecondValue(v.value())),
            },
        },
        Value::Time(v) => match v.unit() {
            TimeUnit::Second => v1::Value {
                value: Some(v1::value::Value::TimeSecondValue(v.value())),
            },
            TimeUnit::Millisecond => v1::Value {
                value: Some(v1::value::Value::TimeMillisecondValue(v.value())),
            },
            TimeUnit::Microsecond => v1::Value {
                value: Some(v1::value::Value::TimeMicrosecondValue(v.value())),
            },
            TimeUnit::Nanosecond => v1::Value {
                value: Some(v1::value::Value::TimeNanosecondValue(v.value())),
            },
        },
        Value::Interval(_) | Value::List(_) => return None,
    };

    Some(proto_value)
}

/// Returns true if the column type is equal to exepcted type.
fn is_column_type_eq(column_type: ColumnDataType, expect_type: &ConcreteDataType) -> bool {
    match (column_type, expect_type) {
        (ColumnDataType::Boolean, ConcreteDataType::Boolean(_))
        | (ColumnDataType::Int8, ConcreteDataType::Int8(_))
        | (ColumnDataType::Int16, ConcreteDataType::Int16(_))
        | (ColumnDataType::Int32, ConcreteDataType::Int32(_))
        | (ColumnDataType::Int64, ConcreteDataType::Int64(_))
        | (ColumnDataType::Uint8, ConcreteDataType::UInt8(_))
        | (ColumnDataType::Uint16, ConcreteDataType::UInt16(_))
        | (ColumnDataType::Uint32, ConcreteDataType::UInt32(_))
        | (ColumnDataType::Uint64, ConcreteDataType::UInt64(_))
        | (ColumnDataType::Float32, ConcreteDataType::Float32(_))
        | (ColumnDataType::Float64, ConcreteDataType::Float64(_))
        | (ColumnDataType::Binary, ConcreteDataType::Binary(_))
        | (ColumnDataType::String, ConcreteDataType::String(_))
        | (ColumnDataType::Date, ConcreteDataType::Date(_))
        | (ColumnDataType::Datetime, ConcreteDataType::DateTime(_))
        | (
            ColumnDataType::TimestampSecond,
            ConcreteDataType::Timestamp(TimestampType::Second(_)),
        )
        | (
            ColumnDataType::TimestampMillisecond,
            ConcreteDataType::Timestamp(TimestampType::Millisecond(_)),
        )
        | (
            ColumnDataType::TimestampMicrosecond,
            ConcreteDataType::Timestamp(TimestampType::Microsecond(_)),
        )
        | (
            ColumnDataType::TimestampNanosecond,
            ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)),
        )
        | (ColumnDataType::TimeSecond, ConcreteDataType::Time(TimeType::Second(_)))
        | (ColumnDataType::TimeMillisecond, ConcreteDataType::Time(TimeType::Millisecond(_)))
        | (ColumnDataType::TimeMicrosecond, ConcreteDataType::Time(TimeType::Microsecond(_)))
        | (ColumnDataType::TimeNanosecond, ConcreteDataType::Time(TimeType::Nanosecond(_))) => true,
        _ => false,
    }
}
