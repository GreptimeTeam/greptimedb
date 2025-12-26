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

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{
    DataType, DurationMicrosecondType, DurationMillisecondType, DurationNanosecondType,
    DurationSecondType, Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
    Time64NanosecondType, TimeUnit, TimestampMicrosecondType, TimestampMillisecondType,
    TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::Array;
use common_time::time::Time;
use common_time::{Duration, Timestamp};

pub type BinaryArray = arrow::array::BinaryArray;
pub type MutableBinaryArray = arrow::array::BinaryBuilder;
pub type StringArray = arrow::array::StringArray;
pub type MutableStringArray = arrow::array::StringBuilder;
pub type LargeStringArray = arrow::array::LargeStringArray;
pub type MutableLargeStringArray = arrow::array::LargeStringBuilder;

/// Get the [Timestamp] value at index `i` of the timestamp array.
///
/// Note: This method does not check for nulls and the value is arbitrary
/// if [`is_null`](arrow::array::Array::is_null) returns true for the index.
///
/// # Panics
/// 1. if index `i` is out of bounds;
/// 2. or the array is not timestamp type.
pub fn timestamp_array_value(array: &ArrayRef, i: usize) -> Timestamp {
    let DataType::Timestamp(time_unit, _) = &array.data_type() else {
        unreachable!()
    };
    let v = match time_unit {
        TimeUnit::Second => {
            let array = array.as_primitive::<TimestampSecondType>();
            array.value(i)
        }
        TimeUnit::Millisecond => {
            let array = array.as_primitive::<TimestampMillisecondType>();
            array.value(i)
        }
        TimeUnit::Microsecond => {
            let array = array.as_primitive::<TimestampMicrosecondType>();
            array.value(i)
        }
        TimeUnit::Nanosecond => {
            let array = array.as_primitive::<TimestampNanosecondType>();
            array.value(i)
        }
    };
    Timestamp::new(v, time_unit.into())
}

/// Get the [Time] value at index `i` of the time array.
///
/// Note: This method does not check for nulls and the value is arbitrary
/// if [`is_null`](arrow::array::Array::is_null) returns true for the index.
///
/// # Panics
/// 1. if index `i` is out of bounds;
/// 2. or the array is not `Time32` or `Time64` type.
pub fn time_array_value(array: &ArrayRef, i: usize) -> Time {
    match array.data_type() {
        DataType::Time32(time_unit) | DataType::Time64(time_unit) => match time_unit {
            TimeUnit::Second => {
                let array = array.as_primitive::<Time32SecondType>();
                Time::new_second(array.value(i) as i64)
            }
            TimeUnit::Millisecond => {
                let array = array.as_primitive::<Time32MillisecondType>();
                Time::new_millisecond(array.value(i) as i64)
            }
            TimeUnit::Microsecond => {
                let array = array.as_primitive::<Time64MicrosecondType>();
                Time::new_microsecond(array.value(i))
            }
            TimeUnit::Nanosecond => {
                let array = array.as_primitive::<Time64NanosecondType>();
                Time::new_nanosecond(array.value(i))
            }
        },
        _ => unreachable!(),
    }
}

/// Get the [Duration] value at index `i` of the duration array.
///
/// Note: This method does not check for nulls and the value is arbitrary
/// if [`is_null`](arrow::array::Array::is_null) returns true for the index.
///
/// # Panics
/// 1. if index `i` is out of bounds;
/// 2. or the array is not duration type.
pub fn duration_array_value(array: &ArrayRef, i: usize) -> Duration {
    let DataType::Duration(time_unit) = array.data_type() else {
        unreachable!();
    };
    let v = match time_unit {
        TimeUnit::Second => {
            let array = array.as_primitive::<DurationSecondType>();
            array.value(i)
        }
        TimeUnit::Millisecond => {
            let array = array.as_primitive::<DurationMillisecondType>();
            array.value(i)
        }
        TimeUnit::Microsecond => {
            let array = array.as_primitive::<DurationMicrosecondType>();
            array.value(i)
        }
        TimeUnit::Nanosecond => {
            let array = array.as_primitive::<DurationNanosecondType>();
            array.value(i)
        }
    };
    Duration::new(v, time_unit.into())
}

/// Get the string value at index `i` for `Utf8`, `LargeUtf8`, or `Utf8View` arrays.
///
/// Returns `None` when the array type is not a string type or the value is null.
///
/// # Panics
///
/// If index `i` is out of bounds.
pub fn string_array_value_at_index(array: &ArrayRef, i: usize) -> Option<&str> {
    match array.data_type() {
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            array.is_valid(i).then(|| array.value(i))
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            array.is_valid(i).then(|| array.value(i))
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            array.is_valid(i).then(|| array.value(i))
        }
        _ => None,
    }
}
