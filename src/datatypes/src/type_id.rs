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

/// Unique identifier for logical data type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalTypeId {
    Null,

    // Numeric types:
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,

    Decimal128,

    // String types:
    String,
    Binary,

    // Date & Time types:
    /// Date representing the elapsed time since UNIX epoch (1970-01-01)
    /// in days (32 bits).
    Date,
    /// Datetime representing the elapsed time since UNIX epoch (1970-01-01) in
    /// seconds/milliseconds/microseconds/nanoseconds, determined by precision.
    DateTime,

    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    /// A 64-bit time representing the elapsed time since midnight in the unit of `TimeUnit`.
    TimeSecond,
    TimeMillisecond,
    TimeMicrosecond,
    TimeNanosecond,
    /// A 64-bit duration representing the elapsed time in either seconds,
    /// milliseconds, microseconds or nanoseconds.
    DurationSecond,
    DurationMillisecond,
    DurationMicrosecond,
    DurationNanosecond,
    /// A 32-bit interval representing the elapsed time in months.
    IntervalYearMonth,
    /// A 64-bit interval representing the elapsed time in days and milliseconds.
    IntervalDayTime,
    /// A 128-bit interval representing the elapsed time in months, days and nanoseconds.
    IntervalMonthDayNano,

    List,
    Dictionary,
}

impl LogicalTypeId {
    /// Create ConcreteDataType based on this id. This method is for test only as it
    /// would lost some info.
    ///
    /// # Panics
    /// Panics if data type is not supported.
    #[cfg(any(test, feature = "test"))]
    pub fn data_type(&self) -> crate::data_type::ConcreteDataType {
        use crate::data_type::ConcreteDataType;

        match self {
            LogicalTypeId::Null => ConcreteDataType::null_datatype(),
            LogicalTypeId::Boolean => ConcreteDataType::boolean_datatype(),
            LogicalTypeId::Int8 => ConcreteDataType::int8_datatype(),
            LogicalTypeId::Int16 => ConcreteDataType::int16_datatype(),
            LogicalTypeId::Int32 => ConcreteDataType::int32_datatype(),
            LogicalTypeId::Int64 => ConcreteDataType::int64_datatype(),
            LogicalTypeId::UInt8 => ConcreteDataType::uint8_datatype(),
            LogicalTypeId::UInt16 => ConcreteDataType::uint16_datatype(),
            LogicalTypeId::UInt32 => ConcreteDataType::uint32_datatype(),
            LogicalTypeId::UInt64 => ConcreteDataType::uint64_datatype(),
            LogicalTypeId::Float32 => ConcreteDataType::float32_datatype(),
            LogicalTypeId::Float64 => ConcreteDataType::float64_datatype(),
            LogicalTypeId::String => ConcreteDataType::string_datatype(),
            LogicalTypeId::Binary => ConcreteDataType::binary_datatype(),
            LogicalTypeId::Date => ConcreteDataType::date_datatype(),
            LogicalTypeId::DateTime => ConcreteDataType::datetime_datatype(),
            LogicalTypeId::TimestampSecond => ConcreteDataType::timestamp_second_datatype(),
            LogicalTypeId::TimestampMillisecond => {
                ConcreteDataType::timestamp_millisecond_datatype()
            }
            LogicalTypeId::TimestampMicrosecond => {
                ConcreteDataType::timestamp_microsecond_datatype()
            }
            LogicalTypeId::TimestampNanosecond => ConcreteDataType::timestamp_nanosecond_datatype(),
            LogicalTypeId::List => {
                ConcreteDataType::list_datatype(ConcreteDataType::null_datatype())
            }
            LogicalTypeId::Dictionary => ConcreteDataType::dictionary_datatype(
                ConcreteDataType::null_datatype(),
                ConcreteDataType::null_datatype(),
            ),
            LogicalTypeId::TimeSecond => ConcreteDataType::time_second_datatype(),
            LogicalTypeId::TimeMillisecond => ConcreteDataType::time_millisecond_datatype(),
            LogicalTypeId::TimeMicrosecond => ConcreteDataType::time_microsecond_datatype(),
            LogicalTypeId::TimeNanosecond => ConcreteDataType::time_nanosecond_datatype(),
            LogicalTypeId::IntervalYearMonth => ConcreteDataType::interval_year_month_datatype(),
            LogicalTypeId::IntervalDayTime => ConcreteDataType::interval_day_time_datatype(),
            LogicalTypeId::IntervalMonthDayNano => {
                ConcreteDataType::interval_month_day_nano_datatype()
            }
            LogicalTypeId::DurationSecond => ConcreteDataType::duration_second_datatype(),
            LogicalTypeId::DurationMillisecond => ConcreteDataType::duration_millisecond_datatype(),
            LogicalTypeId::DurationMicrosecond => ConcreteDataType::duration_microsecond_datatype(),
            LogicalTypeId::DurationNanosecond => ConcreteDataType::duration_nanosecond_datatype(),
            LogicalTypeId::Decimal128 => ConcreteDataType::decimal128_default_datatype(),
        }
    }
}
