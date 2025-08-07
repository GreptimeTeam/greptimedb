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

use std::fmt;
use std::sync::Arc;

use arrow::compute::cast as arrow_array_cast;
use arrow::datatypes::{
    DataType as ArrowDataType, IntervalUnit as ArrowIntervalUnit, TimeUnit as ArrowTimeUnit,
};
use arrow_schema::DECIMAL_DEFAULT_SCALE;
use common_decimal::decimal128::DECIMAL128_MAX_PRECISION;
use common_time::interval::IntervalUnit;
use common_time::timestamp::TimeUnit;
use enum_dispatch::enum_dispatch;
use paste::paste;
use serde::{Deserialize, Serialize};

use crate::error::{self, Error, Result};
use crate::type_id::LogicalTypeId;
use crate::types::{
    BinaryType, BooleanType, DateType, Decimal128Type, DictionaryType, DurationMicrosecondType,
    DurationMillisecondType, DurationNanosecondType, DurationSecondType, DurationType, Float32Type,
    Float64Type, Int16Type, Int32Type, Int64Type, Int8Type, IntervalDayTimeType,
    IntervalMonthDayNanoType, IntervalType, IntervalYearMonthType, JsonType, ListType, NullType,
    StringType, StructType, TimeMillisecondType, TimeType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, TimestampType,
    UInt16Type, UInt32Type, UInt64Type, UInt8Type, VectorType,
};
use crate::value::Value;
use crate::vectors::MutableVector;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[enum_dispatch(DataType)]
pub enum ConcreteDataType {
    Null(NullType),
    Boolean(BooleanType),

    // Numeric types:
    Int8(Int8Type),
    Int16(Int16Type),
    Int32(Int32Type),
    Int64(Int64Type),
    UInt8(UInt8Type),
    UInt16(UInt16Type),
    UInt32(UInt32Type),
    UInt64(UInt64Type),
    Float32(Float32Type),
    Float64(Float64Type),

    // Decimal128 type:
    Decimal128(Decimal128Type),

    // String types:
    Binary(BinaryType),
    String(StringType),

    // Date and time types:
    Date(DateType),
    Timestamp(TimestampType),
    Time(TimeType),

    // Duration type:
    Duration(DurationType),

    // Interval type:
    Interval(IntervalType),

    // Compound types:
    List(ListType),
    Dictionary(DictionaryType),
    Struct(StructType),

    // JSON type:
    Json(JsonType),

    // Vector type:
    Vector(VectorType),
}

impl fmt::Display for ConcreteDataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConcreteDataType::Null(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Boolean(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Int8(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Int16(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Int32(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Int64(v) => write!(f, "{}", v.name()),
            ConcreteDataType::UInt8(v) => write!(f, "{}", v.name()),
            ConcreteDataType::UInt16(v) => write!(f, "{}", v.name()),
            ConcreteDataType::UInt32(v) => write!(f, "{}", v.name()),
            ConcreteDataType::UInt64(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Float32(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Float64(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Binary(v) => write!(f, "{}", v.name()),
            ConcreteDataType::String(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Date(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Timestamp(t) => match t {
                TimestampType::Second(v) => write!(f, "{}", v.name()),
                TimestampType::Millisecond(v) => write!(f, "{}", v.name()),
                TimestampType::Microsecond(v) => write!(f, "{}", v.name()),
                TimestampType::Nanosecond(v) => write!(f, "{}", v.name()),
            },
            ConcreteDataType::Time(t) => match t {
                TimeType::Second(v) => write!(f, "{}", v.name()),
                TimeType::Millisecond(v) => write!(f, "{}", v.name()),
                TimeType::Microsecond(v) => write!(f, "{}", v.name()),
                TimeType::Nanosecond(v) => write!(f, "{}", v.name()),
            },
            ConcreteDataType::Interval(i) => match i {
                IntervalType::YearMonth(v) => write!(f, "{}", v.name()),
                IntervalType::DayTime(v) => write!(f, "{}", v.name()),
                IntervalType::MonthDayNano(v) => write!(f, "{}", v.name()),
            },
            ConcreteDataType::Duration(d) => match d {
                DurationType::Second(v) => write!(f, "{}", v.name()),
                DurationType::Millisecond(v) => write!(f, "{}", v.name()),
                DurationType::Microsecond(v) => write!(f, "{}", v.name()),
                DurationType::Nanosecond(v) => write!(f, "{}", v.name()),
            },
            ConcreteDataType::Decimal128(v) => write!(f, "{}", v.name()),
            ConcreteDataType::List(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Struct(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Dictionary(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Json(v) => write!(f, "{}", v.name()),
            ConcreteDataType::Vector(v) => write!(f, "{}", v.name()),
        }
    }
}

// TODO(yingwen): Refactor these `is_xxx()` methods, such as adding a `properties()` method
// returning all these properties to the `DataType` trait
impl ConcreteDataType {
    pub fn is_float(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::Float64(_) | ConcreteDataType::Float32(_)
        )
    }

    pub fn is_boolean(&self) -> bool {
        matches!(self, ConcreteDataType::Boolean(_))
    }

    pub fn is_string(&self) -> bool {
        matches!(self, ConcreteDataType::String(_))
    }

    pub fn is_stringifiable(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::String(_)
                | ConcreteDataType::Date(_)
                | ConcreteDataType::Timestamp(_)
                | ConcreteDataType::Time(_)
                | ConcreteDataType::Interval(_)
                | ConcreteDataType::Duration(_)
                | ConcreteDataType::Decimal128(_)
                | ConcreteDataType::Binary(_)
                | ConcreteDataType::Json(_)
                | ConcreteDataType::Vector(_)
        )
    }

    pub fn is_signed(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::Int8(_)
                | ConcreteDataType::Int16(_)
                | ConcreteDataType::Int32(_)
                | ConcreteDataType::Int64(_)
                | ConcreteDataType::Date(_)
                | ConcreteDataType::Timestamp(_)
                | ConcreteDataType::Time(_)
                | ConcreteDataType::Interval(_)
                | ConcreteDataType::Duration(_)
                | ConcreteDataType::Decimal128(_)
        )
    }

    pub fn is_unsigned(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::UInt8(_)
                | ConcreteDataType::UInt16(_)
                | ConcreteDataType::UInt32(_)
                | ConcreteDataType::UInt64(_)
        )
    }

    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            ConcreteDataType::Int8(_)
                | ConcreteDataType::Int16(_)
                | ConcreteDataType::Int32(_)
                | ConcreteDataType::Int64(_)
                | ConcreteDataType::UInt8(_)
                | ConcreteDataType::UInt16(_)
                | ConcreteDataType::UInt32(_)
                | ConcreteDataType::UInt64(_)
                | ConcreteDataType::Float32(_)
                | ConcreteDataType::Float64(_)
        )
    }

    pub fn is_timestamp(&self) -> bool {
        matches!(self, ConcreteDataType::Timestamp(_))
    }

    pub fn is_decimal(&self) -> bool {
        matches!(self, ConcreteDataType::Decimal128(_))
    }

    pub fn is_json(&self) -> bool {
        matches!(self, ConcreteDataType::Json(_))
    }

    pub fn is_vector(&self) -> bool {
        matches!(self, ConcreteDataType::Vector(_))
    }

    pub fn numerics() -> Vec<ConcreteDataType> {
        vec![
            ConcreteDataType::int8_datatype(),
            ConcreteDataType::int16_datatype(),
            ConcreteDataType::int32_datatype(),
            ConcreteDataType::int64_datatype(),
            ConcreteDataType::uint8_datatype(),
            ConcreteDataType::uint16_datatype(),
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::uint64_datatype(),
            ConcreteDataType::float32_datatype(),
            ConcreteDataType::float64_datatype(),
        ]
    }

    pub fn unsigned_integers() -> Vec<ConcreteDataType> {
        vec![
            ConcreteDataType::uint8_datatype(),
            ConcreteDataType::uint16_datatype(),
            ConcreteDataType::uint32_datatype(),
            ConcreteDataType::uint64_datatype(),
        ]
    }

    pub fn timestamps() -> Vec<ConcreteDataType> {
        vec![
            ConcreteDataType::timestamp_second_datatype(),
            ConcreteDataType::timestamp_millisecond_datatype(),
            ConcreteDataType::timestamp_microsecond_datatype(),
            ConcreteDataType::timestamp_nanosecond_datatype(),
        ]
    }

    /// Convert arrow data type to [ConcreteDataType].
    ///
    /// # Panics
    /// Panic if given arrow data type is not supported.
    pub fn from_arrow_type(dt: &ArrowDataType) -> Self {
        ConcreteDataType::try_from(dt).expect("Unimplemented type")
    }

    pub fn is_null(&self) -> bool {
        matches!(self, ConcreteDataType::Null(NullType))
    }

    /// Try to cast the type as a [`ListType`].
    pub fn as_list(&self) -> Option<&ListType> {
        match self {
            ConcreteDataType::List(t) => Some(t),
            _ => None,
        }
    }

    /// Try to cast data type as a [`TimestampType`].
    pub fn as_timestamp(&self) -> Option<TimestampType> {
        match self {
            ConcreteDataType::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    /// Try to get numeric precision, returns `None` if it's not numeric type
    pub fn numeric_precision(&self) -> Option<u8> {
        match self {
            ConcreteDataType::Int8(_) | ConcreteDataType::UInt8(_) => Some(3),
            ConcreteDataType::Int16(_) | ConcreteDataType::UInt16(_) => Some(5),
            ConcreteDataType::Int32(_) | ConcreteDataType::UInt32(_) => Some(10),
            ConcreteDataType::Int64(_) => Some(19),
            ConcreteDataType::UInt64(_) => Some(20),
            ConcreteDataType::Float32(_) => Some(12),
            ConcreteDataType::Float64(_) => Some(22),
            ConcreteDataType::Decimal128(decimal_type) => Some(decimal_type.precision()),
            _ => None,
        }
    }

    /// Try to get numeric scale, returns `None` if it's float or not numeric type
    pub fn numeric_scale(&self) -> Option<i8> {
        match self {
            ConcreteDataType::Int8(_)
            | ConcreteDataType::UInt8(_)
            | ConcreteDataType::Int16(_)
            | ConcreteDataType::UInt16(_)
            | ConcreteDataType::Int32(_)
            | ConcreteDataType::UInt32(_)
            | ConcreteDataType::Int64(_)
            | ConcreteDataType::UInt64(_) => Some(0),
            ConcreteDataType::Float32(_) | ConcreteDataType::Float64(_) => None,
            ConcreteDataType::Decimal128(decimal_type) => Some(decimal_type.scale()),
            _ => None,
        }
    }

    /// Try to cast data type as a [`TimeType`].
    pub fn as_time(&self) -> Option<TimeType> {
        match self {
            ConcreteDataType::Int64(_) => Some(TimeType::Millisecond(TimeMillisecondType)),
            ConcreteDataType::Time(t) => Some(*t),
            _ => None,
        }
    }

    pub fn as_decimal128(&self) -> Option<Decimal128Type> {
        match self {
            ConcreteDataType::Decimal128(d) => Some(*d),
            _ => None,
        }
    }

    pub fn as_json(&self) -> Option<JsonType> {
        match self {
            ConcreteDataType::Json(j) => Some(*j),
            _ => None,
        }
    }

    pub fn as_vector(&self) -> Option<VectorType> {
        match self {
            ConcreteDataType::Vector(v) => Some(*v),
            _ => None,
        }
    }

    /// Checks if the data type can cast to another data type.
    pub fn can_arrow_type_cast_to(&self, to_type: &ConcreteDataType) -> bool {
        let array = arrow_array::new_empty_array(&self.as_arrow_type());
        arrow_array_cast(array.as_ref(), &to_type.as_arrow_type()).is_ok()
    }

    /// Try to cast data type as a [`DurationType`].
    pub fn as_duration(&self) -> Option<DurationType> {
        match self {
            ConcreteDataType::Duration(d) => Some(*d),
            _ => None,
        }
    }

    /// Return the datatype name in postgres type system
    pub fn postgres_datatype_name(&self) -> &'static str {
        match self {
            &ConcreteDataType::Null(_) => "UNKNOWN",
            &ConcreteDataType::Boolean(_) => "BOOL",
            &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => "CHAR",
            &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => "INT2",
            &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => "INT4",
            &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => "INT8",
            &ConcreteDataType::Float32(_) => "FLOAT4",
            &ConcreteDataType::Float64(_) => "FLOAT8",
            &ConcreteDataType::Binary(_) | &ConcreteDataType::Vector(_) => "BYTEA",
            &ConcreteDataType::String(_) => "VARCHAR",
            &ConcreteDataType::Date(_) => "DATE",
            &ConcreteDataType::Timestamp(_) => "TIMESTAMP",
            &ConcreteDataType::Time(_) => "TIME",
            &ConcreteDataType::Interval(_) => "INTERVAL",
            &ConcreteDataType::Decimal128(_) => "NUMERIC",
            &ConcreteDataType::Json(_) => "JSON",
            ConcreteDataType::List(list) => match list.item_type() {
                &ConcreteDataType::Null(_) => "UNKNOWN",
                &ConcreteDataType::Boolean(_) => "_BOOL",
                &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => "_CHAR",
                &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => "_INT2",
                &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => "_INT4",
                &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => "_INT8",
                &ConcreteDataType::Float32(_) => "_FLOAT4",
                &ConcreteDataType::Float64(_) => "_FLOAT8",
                &ConcreteDataType::Binary(_) => "_BYTEA",
                &ConcreteDataType::String(_) => "_VARCHAR",
                &ConcreteDataType::Date(_) => "_DATE",
                &ConcreteDataType::Timestamp(_) => "_TIMESTAMP",
                &ConcreteDataType::Time(_) => "_TIME",
                &ConcreteDataType::Interval(_) => "_INTERVAL",
                &ConcreteDataType::Decimal128(_) => "_NUMERIC",
                &ConcreteDataType::Json(_) => "_JSON",
                &ConcreteDataType::Duration(_)
                | &ConcreteDataType::Dictionary(_)
                | &ConcreteDataType::Vector(_)
                | &ConcreteDataType::List(_)
                | &ConcreteDataType::Struct(_) => "UNKNOWN",
            },
            &ConcreteDataType::Duration(_)
            | &ConcreteDataType::Dictionary(_)
            | &ConcreteDataType::Struct(_) => "UNKNOWN",
        }
    }
}

impl From<&ConcreteDataType> for ConcreteDataType {
    fn from(t: &ConcreteDataType) -> Self {
        t.clone()
    }
}

impl TryFrom<&ArrowDataType> for ConcreteDataType {
    type Error = Error;

    fn try_from(dt: &ArrowDataType) -> Result<ConcreteDataType> {
        let concrete_type = match dt {
            ArrowDataType::Null => Self::null_datatype(),
            ArrowDataType::Boolean => Self::boolean_datatype(),
            ArrowDataType::UInt8 => Self::uint8_datatype(),
            ArrowDataType::UInt16 => Self::uint16_datatype(),
            ArrowDataType::UInt32 => Self::uint32_datatype(),
            ArrowDataType::UInt64 => Self::uint64_datatype(),
            ArrowDataType::Int8 => Self::int8_datatype(),
            ArrowDataType::Int16 => Self::int16_datatype(),
            ArrowDataType::Int32 => Self::int32_datatype(),
            ArrowDataType::Int64 => Self::int64_datatype(),
            ArrowDataType::Float32 => Self::float32_datatype(),
            ArrowDataType::Float64 => Self::float64_datatype(),
            ArrowDataType::Date32 => Self::date_datatype(),
            ArrowDataType::Timestamp(u, _) => ConcreteDataType::from_arrow_time_unit(u),
            ArrowDataType::Interval(u) => ConcreteDataType::from_arrow_interval_unit(u),
            ArrowDataType::Binary | ArrowDataType::LargeBinary => Self::binary_datatype(),
            ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 => Self::string_datatype(),
            ArrowDataType::List(field) => Self::List(ListType::new(
                ConcreteDataType::from_arrow_type(field.data_type()),
            )),
            ArrowDataType::Dictionary(key_type, value_type) => {
                let key_type = ConcreteDataType::from_arrow_type(key_type);
                let value_type = ConcreteDataType::from_arrow_type(value_type);
                Self::Dictionary(DictionaryType::new(key_type, value_type))
            }
            ArrowDataType::Time32(u) => ConcreteDataType::Time(TimeType::from_unit(u.into())),
            ArrowDataType::Time64(u) => ConcreteDataType::Time(TimeType::from_unit(u.into())),
            ArrowDataType::Duration(u) => {
                ConcreteDataType::Duration(DurationType::from_unit(u.into()))
            }
            ArrowDataType::Decimal128(precision, scale) => {
                ConcreteDataType::decimal128_datatype(*precision, *scale)
            }
            ArrowDataType::Struct(fields) => ConcreteDataType::Struct(fields.try_into()?),
            ArrowDataType::Float16
            | ArrowDataType::Date64
            | ArrowDataType::FixedSizeBinary(_)
            | ArrowDataType::BinaryView
            | ArrowDataType::Utf8View
            | ArrowDataType::ListView(_)
            | ArrowDataType::FixedSizeList(_, _)
            | ArrowDataType::LargeList(_)
            | ArrowDataType::LargeListView(_)
            | ArrowDataType::Union(_, _)
            | ArrowDataType::Decimal256(_, _)
            | ArrowDataType::Map(_, _)
            | ArrowDataType::RunEndEncoded(_, _)
            | ArrowDataType::Decimal32(_, _)
            | ArrowDataType::Decimal64(_, _) => {
                return error::UnsupportedArrowTypeSnafu {
                    arrow_type: dt.clone(),
                }
                .fail()
            }
        };

        Ok(concrete_type)
    }
}

macro_rules! impl_new_concrete_type_functions {
    ($($Type: ident), +) => {
        paste! {
            impl ConcreteDataType {
                $(
                    pub fn [<$Type:lower _datatype>]() -> ConcreteDataType {
                        ConcreteDataType::$Type([<$Type Type>]::default())
                    }
                )+
            }
        }
    }
}

impl_new_concrete_type_functions!(
    Null, Boolean, UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64,
    Binary, Date, String, Json
);

impl ConcreteDataType {
    pub fn timestamp_second_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Second(TimestampSecondType))
    }

    pub fn timestamp_millisecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Millisecond(TimestampMillisecondType))
    }

    pub fn timestamp_microsecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Microsecond(TimestampMicrosecondType))
    }

    pub fn timestamp_nanosecond_datatype() -> Self {
        ConcreteDataType::Timestamp(TimestampType::Nanosecond(TimestampNanosecondType))
    }

    /// Returns the time data type with `TimeUnit`.
    pub fn time_datatype(unit: TimeUnit) -> Self {
        ConcreteDataType::Time(TimeType::from_unit(unit))
    }

    /// Creates a [Time(TimeSecondType)] datatype.
    pub fn time_second_datatype() -> Self {
        Self::time_datatype(TimeUnit::Second)
    }

    /// Creates a [Time(TimeMillisecondType)] datatype.
    pub fn time_millisecond_datatype() -> Self {
        Self::time_datatype(TimeUnit::Millisecond)
    }

    /// Creates a [Time(TimeMicrosecond)] datatype.
    pub fn time_microsecond_datatype() -> Self {
        Self::time_datatype(TimeUnit::Microsecond)
    }

    /// Creates a [Time(TimeNanosecond)] datatype.
    pub fn time_nanosecond_datatype() -> Self {
        Self::time_datatype(TimeUnit::Nanosecond)
    }

    /// Creates a [Duration(DurationSecondType)] datatype.
    pub fn duration_second_datatype() -> Self {
        ConcreteDataType::Duration(DurationType::Second(DurationSecondType))
    }

    /// Creates a [Duration(DurationMillisecondType)] datatype.
    pub fn duration_millisecond_datatype() -> Self {
        ConcreteDataType::Duration(DurationType::Millisecond(DurationMillisecondType))
    }

    /// Creates a [Duration(DurationMicrosecondType)] datatype.
    pub fn duration_microsecond_datatype() -> Self {
        ConcreteDataType::Duration(DurationType::Microsecond(DurationMicrosecondType))
    }

    /// Creates a [Duration(DurationNanosecondType)] datatype.
    pub fn duration_nanosecond_datatype() -> Self {
        ConcreteDataType::Duration(DurationType::Nanosecond(DurationNanosecondType))
    }

    /// Creates a [Interval(IntervalMonthDayNanoType)] datatype.
    pub fn interval_month_day_nano_datatype() -> Self {
        ConcreteDataType::Interval(IntervalType::MonthDayNano(IntervalMonthDayNanoType))
    }

    /// Creates a [Interval(IntervalYearMonthType)] datatype.
    pub fn interval_year_month_datatype() -> Self {
        ConcreteDataType::Interval(IntervalType::YearMonth(IntervalYearMonthType))
    }

    /// Creates a [Interval(IntervalDayTimeType)] datatype.
    pub fn interval_day_time_datatype() -> Self {
        ConcreteDataType::Interval(IntervalType::DayTime(IntervalDayTimeType))
    }

    pub fn timestamp_datatype(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::timestamp_second_datatype(),
            TimeUnit::Millisecond => Self::timestamp_millisecond_datatype(),
            TimeUnit::Microsecond => Self::timestamp_microsecond_datatype(),
            TimeUnit::Nanosecond => Self::timestamp_nanosecond_datatype(),
        }
    }

    /// Converts from arrow timestamp unit to
    pub fn from_arrow_time_unit(t: &ArrowTimeUnit) -> Self {
        match t {
            ArrowTimeUnit::Second => Self::timestamp_second_datatype(),
            ArrowTimeUnit::Millisecond => Self::timestamp_millisecond_datatype(),
            ArrowTimeUnit::Microsecond => Self::timestamp_microsecond_datatype(),
            ArrowTimeUnit::Nanosecond => Self::timestamp_nanosecond_datatype(),
        }
    }

    pub fn duration_datatype(unit: TimeUnit) -> Self {
        match unit {
            TimeUnit::Second => Self::duration_second_datatype(),
            TimeUnit::Millisecond => Self::duration_millisecond_datatype(),
            TimeUnit::Microsecond => Self::duration_microsecond_datatype(),
            TimeUnit::Nanosecond => Self::duration_nanosecond_datatype(),
        }
    }

    pub fn interval_datatype(unit: IntervalUnit) -> Self {
        match unit {
            IntervalUnit::YearMonth => Self::interval_year_month_datatype(),
            IntervalUnit::DayTime => Self::interval_day_time_datatype(),
            IntervalUnit::MonthDayNano => Self::interval_month_day_nano_datatype(),
        }
    }

    pub fn from_arrow_interval_unit(u: &ArrowIntervalUnit) -> Self {
        match u {
            ArrowIntervalUnit::YearMonth => Self::interval_year_month_datatype(),
            ArrowIntervalUnit::DayTime => Self::interval_day_time_datatype(),
            ArrowIntervalUnit::MonthDayNano => Self::interval_month_day_nano_datatype(),
        }
    }

    pub fn list_datatype(item_type: ConcreteDataType) -> ConcreteDataType {
        ConcreteDataType::List(ListType::new(item_type))
    }

    pub fn struct_datatype(fields: StructType) -> ConcreteDataType {
        ConcreteDataType::Struct(fields)
    }

    pub fn dictionary_datatype(
        key_type: ConcreteDataType,
        value_type: ConcreteDataType,
    ) -> ConcreteDataType {
        ConcreteDataType::Dictionary(DictionaryType::new(key_type, value_type))
    }

    pub fn decimal128_datatype(precision: u8, scale: i8) -> ConcreteDataType {
        ConcreteDataType::Decimal128(Decimal128Type::new(precision, scale))
    }

    pub fn decimal128_default_datatype() -> ConcreteDataType {
        Self::decimal128_datatype(DECIMAL128_MAX_PRECISION, DECIMAL_DEFAULT_SCALE)
    }

    pub fn vector_datatype(dim: u32) -> ConcreteDataType {
        ConcreteDataType::Vector(VectorType::new(dim))
    }

    pub fn vector_default_datatype() -> ConcreteDataType {
        Self::vector_datatype(0)
    }
}

/// Data type abstraction.
#[enum_dispatch::enum_dispatch]
pub trait DataType: std::fmt::Debug + Send + Sync {
    /// Name of this data type.
    fn name(&self) -> String;

    /// Returns id of the Logical data type.
    fn logical_type_id(&self) -> LogicalTypeId;

    /// Returns the default value of this type.
    fn default_value(&self) -> Value;

    /// Convert this type as [arrow::datatypes::DataType].
    fn as_arrow_type(&self) -> ArrowDataType;

    /// Creates a mutable vector with given `capacity` of this type.
    fn create_mutable_vector(&self, capacity: usize) -> Box<dyn MutableVector>;

    /// Casts the value to specific DataType.
    /// Return None if cast failed.
    fn try_cast(&self, from: Value) -> Option<Value>;
}

pub type DataTypeRef = Arc<dyn DataType>;

#[cfg(test)]
mod tests {
    use arrow::datatypes::Field;

    use super::*;

    #[test]
    fn test_concrete_type_as_datatype_trait() {
        let concrete_type = ConcreteDataType::boolean_datatype();

        assert_eq!("Boolean", concrete_type.to_string());
        assert_eq!(Value::Boolean(false), concrete_type.default_value());
        assert_eq!(LogicalTypeId::Boolean, concrete_type.logical_type_id());
        assert_eq!(ArrowDataType::Boolean, concrete_type.as_arrow_type());
    }

    #[test]
    fn test_from_arrow_type() {
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Null),
            ConcreteDataType::Null(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Boolean),
            ConcreteDataType::Boolean(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Binary),
            ConcreteDataType::Binary(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::LargeBinary),
            ConcreteDataType::Binary(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int8),
            ConcreteDataType::Int8(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int16),
            ConcreteDataType::Int16(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int32),
            ConcreteDataType::Int32(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Int64),
            ConcreteDataType::Int64(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt8),
            ConcreteDataType::UInt8(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt16),
            ConcreteDataType::UInt16(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt32),
            ConcreteDataType::UInt32(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::UInt64),
            ConcreteDataType::UInt64(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Float32),
            ConcreteDataType::Float32(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Float64),
            ConcreteDataType::Float64(_)
        ));
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Utf8),
            ConcreteDataType::String(_)
        ));
        assert_eq!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::List(Arc::new(Field::new(
                "item",
                ArrowDataType::Int32,
                true,
            )))),
            ConcreteDataType::List(ListType::new(ConcreteDataType::int32_datatype()))
        );
        assert!(matches!(
            ConcreteDataType::from_arrow_type(&ArrowDataType::Date32),
            ConcreteDataType::Date(_)
        ));
    }

    #[test]
    fn test_from_arrow_timestamp() {
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Millisecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_microsecond_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Microsecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_nanosecond_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Nanosecond)
        );
        assert_eq!(
            ConcreteDataType::timestamp_second_datatype(),
            ConcreteDataType::from_arrow_time_unit(&ArrowTimeUnit::Second)
        );
    }

    #[test]
    fn test_is_null() {
        assert!(ConcreteDataType::null_datatype().is_null());
        assert!(!ConcreteDataType::int32_datatype().is_null());
    }

    #[test]
    fn test_is_float() {
        assert!(!ConcreteDataType::int32_datatype().is_float());
        assert!(ConcreteDataType::float32_datatype().is_float());
        assert!(ConcreteDataType::float64_datatype().is_float());
    }

    #[test]
    fn test_is_boolean() {
        assert!(!ConcreteDataType::int32_datatype().is_boolean());
        assert!(!ConcreteDataType::float32_datatype().is_boolean());
        assert!(ConcreteDataType::boolean_datatype().is_boolean());
    }

    #[test]
    fn test_is_decimal() {
        assert!(!ConcreteDataType::int32_datatype().is_decimal());
        assert!(!ConcreteDataType::float32_datatype().is_decimal());
        assert!(ConcreteDataType::decimal128_datatype(10, 2).is_decimal());
        assert!(ConcreteDataType::decimal128_datatype(18, 6).is_decimal());
    }

    #[test]
    fn test_is_stringifiable() {
        assert!(!ConcreteDataType::int32_datatype().is_stringifiable());
        assert!(!ConcreteDataType::float32_datatype().is_stringifiable());
        assert!(ConcreteDataType::string_datatype().is_stringifiable());
        assert!(ConcreteDataType::binary_datatype().is_stringifiable());
        assert!(ConcreteDataType::date_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_second_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_millisecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_microsecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::timestamp_nanosecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::time_second_datatype().is_stringifiable());
        assert!(ConcreteDataType::time_millisecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::time_microsecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::time_nanosecond_datatype().is_stringifiable());

        assert!(ConcreteDataType::interval_year_month_datatype().is_stringifiable());
        assert!(ConcreteDataType::interval_day_time_datatype().is_stringifiable());
        assert!(ConcreteDataType::interval_month_day_nano_datatype().is_stringifiable());

        assert!(ConcreteDataType::duration_second_datatype().is_stringifiable());
        assert!(ConcreteDataType::duration_millisecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::duration_microsecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::duration_nanosecond_datatype().is_stringifiable());
        assert!(ConcreteDataType::decimal128_datatype(10, 2).is_stringifiable());
        assert!(ConcreteDataType::vector_default_datatype().is_stringifiable());
    }

    #[test]
    fn test_is_signed() {
        assert!(ConcreteDataType::int8_datatype().is_signed());
        assert!(ConcreteDataType::int16_datatype().is_signed());
        assert!(ConcreteDataType::int32_datatype().is_signed());
        assert!(ConcreteDataType::int64_datatype().is_signed());
        assert!(ConcreteDataType::date_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_second_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_millisecond_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_microsecond_datatype().is_signed());
        assert!(ConcreteDataType::timestamp_nanosecond_datatype().is_signed());
        assert!(ConcreteDataType::time_second_datatype().is_signed());
        assert!(ConcreteDataType::time_millisecond_datatype().is_signed());
        assert!(ConcreteDataType::time_microsecond_datatype().is_signed());
        assert!(ConcreteDataType::time_nanosecond_datatype().is_signed());
        assert!(ConcreteDataType::interval_year_month_datatype().is_signed());
        assert!(ConcreteDataType::interval_day_time_datatype().is_signed());
        assert!(ConcreteDataType::interval_month_day_nano_datatype().is_signed());
        assert!(ConcreteDataType::duration_second_datatype().is_signed());
        assert!(ConcreteDataType::duration_millisecond_datatype().is_signed());
        assert!(ConcreteDataType::duration_microsecond_datatype().is_signed());
        assert!(ConcreteDataType::duration_nanosecond_datatype().is_signed());

        assert!(!ConcreteDataType::uint8_datatype().is_signed());
        assert!(!ConcreteDataType::uint16_datatype().is_signed());
        assert!(!ConcreteDataType::uint32_datatype().is_signed());
        assert!(!ConcreteDataType::uint64_datatype().is_signed());

        assert!(!ConcreteDataType::float32_datatype().is_signed());
        assert!(!ConcreteDataType::float64_datatype().is_signed());

        assert!(ConcreteDataType::decimal128_datatype(10, 2).is_signed());
    }

    #[test]
    fn test_is_unsigned() {
        assert!(!ConcreteDataType::int8_datatype().is_unsigned());
        assert!(!ConcreteDataType::int16_datatype().is_unsigned());
        assert!(!ConcreteDataType::int32_datatype().is_unsigned());
        assert!(!ConcreteDataType::int64_datatype().is_unsigned());
        assert!(!ConcreteDataType::date_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_second_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_millisecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_microsecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::timestamp_nanosecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::time_second_datatype().is_unsigned());
        assert!(!ConcreteDataType::time_millisecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::time_microsecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::time_nanosecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::interval_year_month_datatype().is_unsigned());
        assert!(!ConcreteDataType::interval_day_time_datatype().is_unsigned());
        assert!(!ConcreteDataType::interval_month_day_nano_datatype().is_unsigned());
        assert!(!ConcreteDataType::duration_second_datatype().is_unsigned());
        assert!(!ConcreteDataType::duration_millisecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::duration_microsecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::duration_nanosecond_datatype().is_unsigned());
        assert!(!ConcreteDataType::decimal128_datatype(10, 2).is_unsigned());

        assert!(ConcreteDataType::uint8_datatype().is_unsigned());
        assert!(ConcreteDataType::uint16_datatype().is_unsigned());
        assert!(ConcreteDataType::uint32_datatype().is_unsigned());
        assert!(ConcreteDataType::uint64_datatype().is_unsigned());

        assert!(!ConcreteDataType::float32_datatype().is_unsigned());
        assert!(!ConcreteDataType::float64_datatype().is_unsigned());
    }

    #[test]
    fn test_numerics() {
        let nums = ConcreteDataType::numerics();
        assert_eq!(10, nums.len());
    }

    #[test]
    fn test_as_list() {
        let list_type = ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype());
        assert_eq!(
            ListType::new(ConcreteDataType::int32_datatype()),
            *list_type.as_list().unwrap()
        );
        assert!(ConcreteDataType::int32_datatype().as_list().is_none());
    }

    #[test]
    fn test_display_concrete_data_type() {
        assert_eq!(ConcreteDataType::null_datatype().to_string(), "Null");
        assert_eq!(ConcreteDataType::boolean_datatype().to_string(), "Boolean");
        assert_eq!(ConcreteDataType::binary_datatype().to_string(), "Binary");
        assert_eq!(ConcreteDataType::int8_datatype().to_string(), "Int8");
        assert_eq!(ConcreteDataType::int16_datatype().to_string(), "Int16");
        assert_eq!(ConcreteDataType::int32_datatype().to_string(), "Int32");
        assert_eq!(ConcreteDataType::int64_datatype().to_string(), "Int64");
        assert_eq!(ConcreteDataType::uint8_datatype().to_string(), "UInt8");
        assert_eq!(ConcreteDataType::uint16_datatype().to_string(), "UInt16");
        assert_eq!(ConcreteDataType::uint32_datatype().to_string(), "UInt32");
        assert_eq!(ConcreteDataType::uint64_datatype().to_string(), "UInt64");
        assert_eq!(ConcreteDataType::float32_datatype().to_string(), "Float32");
        assert_eq!(ConcreteDataType::float64_datatype().to_string(), "Float64");
        assert_eq!(ConcreteDataType::string_datatype().to_string(), "String");
        assert_eq!(ConcreteDataType::date_datatype().to_string(), "Date");
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype().to_string(),
            "TimestampMillisecond"
        );
        assert_eq!(
            ConcreteDataType::time_millisecond_datatype().to_string(),
            "TimeMillisecond"
        );
        assert_eq!(
            ConcreteDataType::interval_month_day_nano_datatype().to_string(),
            "IntervalMonthDayNano"
        );
        assert_eq!(
            ConcreteDataType::duration_second_datatype().to_string(),
            "DurationSecond"
        );
        assert_eq!(
            ConcreteDataType::decimal128_datatype(10, 2).to_string(),
            "Decimal(10, 2)"
        );
        // Nested types
        assert_eq!(
            ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype()).to_string(),
            "List<Int32>"
        );
        assert_eq!(
            ConcreteDataType::list_datatype(ConcreteDataType::Dictionary(DictionaryType::new(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype()
            )))
            .to_string(),
            "List<Dictionary<Int32, String>>"
        );
        assert_eq!(
            ConcreteDataType::list_datatype(ConcreteDataType::list_datatype(
                ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype())
            ))
            .to_string(),
            "List<List<List<Int32>>>"
        );
        assert_eq!(
            ConcreteDataType::dictionary_datatype(
                ConcreteDataType::int32_datatype(),
                ConcreteDataType::string_datatype()
            )
            .to_string(),
            "Dictionary<Int32, String>"
        );
        assert_eq!(
            ConcreteDataType::vector_datatype(3).to_string(),
            "Vector(3)"
        );
    }
}
