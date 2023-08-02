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

use datatypes::prelude::ConcreteDataType;
use datatypes::types::{TimeType, TimestampType};
use greptime_proto::v1::ColumnDataType;

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
