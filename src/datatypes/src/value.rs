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

use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use arrow::datatypes::{DataType as ArrowDataType, Field};
use arrow_array::{Array, ListArray};
use base64::engine::general_purpose::URL_SAFE;
use base64::Engine as _;
use common_base::bytes::{Bytes, StringBytes};
use common_decimal::Decimal128;
use common_telemetry::error;
use common_time::date::Date;
use common_time::datetime::DateTime;
use common_time::interval::IntervalUnit;
use common_time::time::Time;
use common_time::timestamp::{TimeUnit, Timestamp};
use common_time::{Duration, Interval, Timezone};
use datafusion_common::ScalarValue;
use greptime_proto::v1::value::ValueData;
pub use ordered_float::OrderedFloat;
use serde::{Deserialize, Serialize, Serializer};
use serde_json::{Number, Value as JsonValue};
use snafu::{ensure, ResultExt};

use crate::error::{self, ConvertArrowArrayToScalarsSnafu, Error, Result, TryFromValueSnafu};
use crate::prelude::*;
use crate::schema::ColumnSchema;
use crate::type_id::LogicalTypeId;
use crate::types::{IntervalType, ListType};
use crate::vectors::ListVector;

pub type OrderedF32 = OrderedFloat<f32>;
pub type OrderedF64 = OrderedFloat<f64>;

/// Value holds a single arbitrary value of any [DataType](crate::data_type::DataType).
///
/// Comparison between values with different types (expect Null) is not allowed.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Value {
    Null,

    // Numeric types:
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedF32),
    Float64(OrderedF64),

    // Decimal type:
    Decimal128(Decimal128),

    // String types:
    String(StringBytes),
    Binary(Bytes),

    // Date & Time types:
    Date(Date),
    DateTime(DateTime),
    Timestamp(Timestamp),
    Time(Time),
    Duration(Duration),
    Interval(Interval),

    List(ListValue),
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "{}", self.data_type().name()),
            Value::Boolean(v) => write!(f, "{v}"),
            Value::UInt8(v) => write!(f, "{v}"),
            Value::UInt16(v) => write!(f, "{v}"),
            Value::UInt32(v) => write!(f, "{v}"),
            Value::UInt64(v) => write!(f, "{v}"),
            Value::Int8(v) => write!(f, "{v}"),
            Value::Int16(v) => write!(f, "{v}"),
            Value::Int32(v) => write!(f, "{v}"),
            Value::Int64(v) => write!(f, "{v}"),
            Value::Float32(v) => write!(f, "{v}"),
            Value::Float64(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{}", v.as_utf8()),
            Value::Binary(v) => {
                let hex = v
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .collect::<Vec<String>>()
                    .join("");
                write!(f, "{hex}")
            }
            Value::Date(v) => write!(f, "{v}"),
            Value::DateTime(v) => write!(f, "{v}"),
            Value::Timestamp(v) => write!(f, "{}", v.to_iso8601_string()),
            Value::Time(t) => write!(f, "{}", t.to_iso8601_string()),
            Value::Interval(v) => write!(f, "{}", v.to_iso8601_string()),
            Value::Duration(d) => write!(f, "{d}"),
            Value::List(v) => {
                let items = v
                    .items()
                    .iter()
                    .map(|i| i.to_string())
                    .collect::<Vec<String>>()
                    .join(", ");
                write!(f, "{}[{}]", v.datatype.name(), items)
            }
            Value::Decimal128(v) => write!(f, "{}", v),
        }
    }
}

macro_rules! define_data_type_func {
    ($struct: ident) => {
        /// Returns data type of the value.
        ///
        /// # Panics
        /// Panics if the data type is not supported.
        pub fn data_type(&self) -> ConcreteDataType {
            match self {
                $struct::Null => ConcreteDataType::null_datatype(),
                $struct::Boolean(_) => ConcreteDataType::boolean_datatype(),
                $struct::UInt8(_) => ConcreteDataType::uint8_datatype(),
                $struct::UInt16(_) => ConcreteDataType::uint16_datatype(),
                $struct::UInt32(_) => ConcreteDataType::uint32_datatype(),
                $struct::UInt64(_) => ConcreteDataType::uint64_datatype(),
                $struct::Int8(_) => ConcreteDataType::int8_datatype(),
                $struct::Int16(_) => ConcreteDataType::int16_datatype(),
                $struct::Int32(_) => ConcreteDataType::int32_datatype(),
                $struct::Int64(_) => ConcreteDataType::int64_datatype(),
                $struct::Float32(_) => ConcreteDataType::float32_datatype(),
                $struct::Float64(_) => ConcreteDataType::float64_datatype(),
                $struct::String(_) => ConcreteDataType::string_datatype(),
                $struct::Binary(_) => ConcreteDataType::binary_datatype(),
                $struct::Date(_) => ConcreteDataType::date_datatype(),
                $struct::DateTime(_) => ConcreteDataType::datetime_datatype(),
                $struct::Time(t) => ConcreteDataType::time_datatype(*t.unit()),
                $struct::Timestamp(v) => ConcreteDataType::timestamp_datatype(v.unit()),
                $struct::Interval(v) => ConcreteDataType::interval_datatype(v.unit()),
                $struct::List(list) => ConcreteDataType::list_datatype(list.datatype().clone()),
                $struct::Duration(d) => ConcreteDataType::duration_datatype(d.unit()),
                $struct::Decimal128(d) => {
                    ConcreteDataType::decimal128_datatype(d.precision(), d.scale())
                }
            }
        }
    };
}

impl Value {
    define_data_type_func!(Value);

    /// Returns true if this is a null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Cast itself to [ListValue].
    pub fn as_list(&self) -> Result<Option<&ListValue>> {
        match self {
            Value::Null => Ok(None),
            Value::List(v) => Ok(Some(v)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast {other:?} to list value"),
            }
            .fail(),
        }
    }

    /// Cast itself to [ValueRef].
    pub fn as_value_ref(&self) -> ValueRef {
        match self {
            Value::Null => ValueRef::Null,
            Value::Boolean(v) => ValueRef::Boolean(*v),
            Value::UInt8(v) => ValueRef::UInt8(*v),
            Value::UInt16(v) => ValueRef::UInt16(*v),
            Value::UInt32(v) => ValueRef::UInt32(*v),
            Value::UInt64(v) => ValueRef::UInt64(*v),
            Value::Int8(v) => ValueRef::Int8(*v),
            Value::Int16(v) => ValueRef::Int16(*v),
            Value::Int32(v) => ValueRef::Int32(*v),
            Value::Int64(v) => ValueRef::Int64(*v),
            Value::Float32(v) => ValueRef::Float32(*v),
            Value::Float64(v) => ValueRef::Float64(*v),
            Value::String(v) => ValueRef::String(v.as_utf8()),
            Value::Binary(v) => ValueRef::Binary(v),
            Value::Date(v) => ValueRef::Date(*v),
            Value::DateTime(v) => ValueRef::DateTime(*v),
            Value::List(v) => ValueRef::List(ListValueRef::Ref { val: v }),
            Value::Timestamp(v) => ValueRef::Timestamp(*v),
            Value::Time(v) => ValueRef::Time(*v),
            Value::Interval(v) => ValueRef::Interval(*v),
            Value::Duration(v) => ValueRef::Duration(*v),
            Value::Decimal128(v) => ValueRef::Decimal128(*v),
        }
    }

    /// Cast Value to timestamp. Return None if value is not a valid timestamp data type.
    pub fn as_timestamp(&self) -> Option<Timestamp> {
        match self {
            Value::Timestamp(t) => Some(*t),
            _ => None,
        }
    }

    /// Cast Value to Interval. Return None if value is not a valid interval data type.
    pub fn as_interval(&self) -> Option<Interval> {
        match self {
            Value::Interval(i) => Some(*i),
            _ => None,
        }
    }

    /// Cast Value to utf8 String. Return None if value is not a valid string data type.
    pub fn as_string(&self) -> Option<String> {
        match self {
            Value::String(bytes) => Some(bytes.as_utf8().to_string()),
            _ => None,
        }
    }

    /// Cast Value to Date. Return None if value is not a valid date data type.
    pub fn as_date(&self) -> Option<Date> {
        match self {
            Value::Date(t) => Some(*t),
            _ => None,
        }
    }

    /// Cast Value to DateTime. Return None if value is not a valid datetime data type.
    pub fn as_datetime(&self) -> Option<DateTime> {
        match self {
            Value::DateTime(t) => Some(*t),
            _ => None,
        }
    }

    /// Cast Value to [Time]. Return None if value is not a valid time data type.
    pub fn as_time(&self) -> Option<Time> {
        match self {
            Value::Int64(v) => Some(Time::new_millisecond(*v)),
            Value::Time(t) => Some(*t),
            _ => None,
        }
    }

    /// Cast Value to u64. Return None if value is not a valid uint64 data type.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Value::UInt8(v) => Some(*v as _),
            Value::UInt16(v) => Some(*v as _),
            Value::UInt32(v) => Some(*v as _),
            Value::UInt64(v) => Some(*v),
            _ => None,
        }
    }

    /// Cast Value to f64. Return None if it's not castable;
    pub fn as_f64_lossy(&self) -> Option<f64> {
        match self {
            Value::Float32(v) => Some(v.0 as _),
            Value::Float64(v) => Some(v.0),
            Value::Int8(v) => Some(*v as _),
            Value::Int16(v) => Some(*v as _),
            Value::Int32(v) => Some(*v as _),
            Value::Int64(v) => Some(*v as _),
            Value::UInt8(v) => Some(*v as _),
            Value::UInt16(v) => Some(*v as _),
            Value::UInt32(v) => Some(*v as _),
            Value::UInt64(v) => Some(*v as _),
            _ => None,
        }
    }

    /// Returns the logical type of the value.
    pub fn logical_type_id(&self) -> LogicalTypeId {
        match self {
            Value::Null => LogicalTypeId::Null,
            Value::Boolean(_) => LogicalTypeId::Boolean,
            Value::UInt8(_) => LogicalTypeId::UInt8,
            Value::UInt16(_) => LogicalTypeId::UInt16,
            Value::UInt32(_) => LogicalTypeId::UInt32,
            Value::UInt64(_) => LogicalTypeId::UInt64,
            Value::Int8(_) => LogicalTypeId::Int8,
            Value::Int16(_) => LogicalTypeId::Int16,
            Value::Int32(_) => LogicalTypeId::Int32,
            Value::Int64(_) => LogicalTypeId::Int64,
            Value::Float32(_) => LogicalTypeId::Float32,
            Value::Float64(_) => LogicalTypeId::Float64,
            Value::String(_) => LogicalTypeId::String,
            Value::Binary(_) => LogicalTypeId::Binary,
            Value::List(_) => LogicalTypeId::List,
            Value::Date(_) => LogicalTypeId::Date,
            Value::DateTime(_) => LogicalTypeId::DateTime,
            Value::Timestamp(t) => match t.unit() {
                TimeUnit::Second => LogicalTypeId::TimestampSecond,
                TimeUnit::Millisecond => LogicalTypeId::TimestampMillisecond,
                TimeUnit::Microsecond => LogicalTypeId::TimestampMicrosecond,
                TimeUnit::Nanosecond => LogicalTypeId::TimestampNanosecond,
            },
            Value::Time(t) => match t.unit() {
                TimeUnit::Second => LogicalTypeId::TimeSecond,
                TimeUnit::Millisecond => LogicalTypeId::TimeMillisecond,
                TimeUnit::Microsecond => LogicalTypeId::TimeMicrosecond,
                TimeUnit::Nanosecond => LogicalTypeId::TimeNanosecond,
            },
            Value::Interval(v) => match v.unit() {
                IntervalUnit::YearMonth => LogicalTypeId::IntervalYearMonth,
                IntervalUnit::DayTime => LogicalTypeId::IntervalDayTime,
                IntervalUnit::MonthDayNano => LogicalTypeId::IntervalMonthDayNano,
            },
            Value::Duration(d) => match d.unit() {
                TimeUnit::Second => LogicalTypeId::DurationSecond,
                TimeUnit::Millisecond => LogicalTypeId::DurationMillisecond,
                TimeUnit::Microsecond => LogicalTypeId::DurationMicrosecond,
                TimeUnit::Nanosecond => LogicalTypeId::DurationNanosecond,
            },
            Value::Decimal128(_) => LogicalTypeId::Decimal128,
        }
    }

    /// Convert the value into [`ScalarValue`] according to the `output_type`.
    pub fn try_to_scalar_value(&self, output_type: &ConcreteDataType) -> Result<ScalarValue> {
        // Compare logical type, since value might not contains full type information.
        let value_type_id = self.logical_type_id();
        let output_type_id = output_type.logical_type_id();
        ensure!(
            // Json type leverage Value(Binary) for storage.
            output_type_id == value_type_id || self.is_null() || (output_type_id == LogicalTypeId::Json && value_type_id == LogicalTypeId::Binary),
            error::ToScalarValueSnafu {
                reason: format!(
                    "expect value to return output_type {output_type_id:?}, actual: {value_type_id:?}",
                ),
            }
        );

        let scalar_value = match self {
            Value::Boolean(v) => ScalarValue::Boolean(Some(*v)),
            Value::UInt8(v) => ScalarValue::UInt8(Some(*v)),
            Value::UInt16(v) => ScalarValue::UInt16(Some(*v)),
            Value::UInt32(v) => ScalarValue::UInt32(Some(*v)),
            Value::UInt64(v) => ScalarValue::UInt64(Some(*v)),
            Value::Int8(v) => ScalarValue::Int8(Some(*v)),
            Value::Int16(v) => ScalarValue::Int16(Some(*v)),
            Value::Int32(v) => ScalarValue::Int32(Some(*v)),
            Value::Int64(v) => ScalarValue::Int64(Some(*v)),
            Value::Float32(v) => ScalarValue::Float32(Some(v.0)),
            Value::Float64(v) => ScalarValue::Float64(Some(v.0)),
            Value::String(v) => ScalarValue::Utf8(Some(v.as_utf8().to_string())),
            Value::Binary(v) => ScalarValue::Binary(Some(v.to_vec())),
            Value::Date(v) => ScalarValue::Date32(Some(v.val())),
            Value::DateTime(v) => ScalarValue::Date64(Some(v.val())),
            Value::Null => to_null_scalar_value(output_type)?,
            Value::List(list) => {
                // Safety: The logical type of the value and output_type are the same.
                let list_type = output_type.as_list().unwrap();
                list.try_to_scalar_value(list_type)?
            }
            Value::Timestamp(t) => timestamp_to_scalar_value(t.unit(), Some(t.value())),
            Value::Time(t) => time_to_scalar_value(*t.unit(), Some(t.value()))?,
            Value::Interval(v) => match v.unit() {
                IntervalUnit::YearMonth => ScalarValue::IntervalYearMonth(Some(v.to_i32())),
                IntervalUnit::DayTime => ScalarValue::IntervalDayTime(Some(v.to_i64())),
                IntervalUnit::MonthDayNano => ScalarValue::IntervalMonthDayNano(Some(v.to_i128())),
            },
            Value::Duration(d) => duration_to_scalar_value(d.unit(), Some(d.value())),
            Value::Decimal128(d) => {
                let (v, p, s) = d.to_scalar_value();
                ScalarValue::Decimal128(v, p, s)
            }
        };

        Ok(scalar_value)
    }

    /// Apply `-` unary op if possible
    pub fn try_negative(&self) -> Option<Self> {
        match self {
            Value::Null => Some(Value::Null),
            Value::UInt8(x) => {
                if *x == 0 {
                    Some(Value::UInt8(*x))
                } else {
                    None
                }
            }
            Value::UInt16(x) => {
                if *x == 0 {
                    Some(Value::UInt16(*x))
                } else {
                    None
                }
            }
            Value::UInt32(x) => {
                if *x == 0 {
                    Some(Value::UInt32(*x))
                } else {
                    None
                }
            }
            Value::UInt64(x) => {
                if *x == 0 {
                    Some(Value::UInt64(*x))
                } else {
                    None
                }
            }
            Value::Int8(x) => Some(Value::Int8(-*x)),
            Value::Int16(x) => Some(Value::Int16(-*x)),
            Value::Int32(x) => Some(Value::Int32(-*x)),
            Value::Int64(x) => Some(Value::Int64(-*x)),
            Value::Float32(x) => Some(Value::Float32(-*x)),
            Value::Float64(x) => Some(Value::Float64(-*x)),
            Value::Decimal128(x) => Some(Value::Decimal128(x.negative())),
            Value::Date(x) => Some(Value::Date(x.negative())),
            Value::DateTime(x) => Some(Value::DateTime(x.negative())),
            Value::Timestamp(x) => Some(Value::Timestamp(x.negative())),
            Value::Time(x) => Some(Value::Time(x.negative())),
            Value::Duration(x) => Some(Value::Duration(x.negative())),
            Value::Interval(x) => Some(Value::Interval(x.negative())),

            Value::Binary(_) | Value::String(_) | Value::Boolean(_) | Value::List(_) => None,
        }
    }
}

pub trait TryAsPrimitive<T: LogicalPrimitiveType> {
    fn try_as_primitive(&self) -> Option<T::Native>;
}

macro_rules! impl_try_as_primitive {
    ($Type: ident, $Variant: ident) => {
        impl TryAsPrimitive<crate::types::$Type> for Value {
            fn try_as_primitive(
                &self,
            ) -> Option<<crate::types::$Type as crate::types::LogicalPrimitiveType>::Native> {
                match self {
                    Value::$Variant(v) => Some((*v).into()),
                    _ => None,
                }
            }
        }
    };
}

impl_try_as_primitive!(Int8Type, Int8);
impl_try_as_primitive!(Int16Type, Int16);
impl_try_as_primitive!(Int32Type, Int32);
impl_try_as_primitive!(Int64Type, Int64);
impl_try_as_primitive!(UInt8Type, UInt8);
impl_try_as_primitive!(UInt16Type, UInt16);
impl_try_as_primitive!(UInt32Type, UInt32);
impl_try_as_primitive!(UInt64Type, UInt64);
impl_try_as_primitive!(Float32Type, Float32);
impl_try_as_primitive!(Float64Type, Float64);

pub fn to_null_scalar_value(output_type: &ConcreteDataType) -> Result<ScalarValue> {
    Ok(match output_type {
        ConcreteDataType::Null(_) => ScalarValue::Null,
        ConcreteDataType::Boolean(_) => ScalarValue::Boolean(None),
        ConcreteDataType::Int8(_) => ScalarValue::Int8(None),
        ConcreteDataType::Int16(_) => ScalarValue::Int16(None),
        ConcreteDataType::Int32(_) => ScalarValue::Int32(None),
        ConcreteDataType::Int64(_) => ScalarValue::Int64(None),
        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(None),
        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(None),
        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(None),
        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(None),
        ConcreteDataType::Float32(_) => ScalarValue::Float32(None),
        ConcreteDataType::Float64(_) => ScalarValue::Float64(None),
        ConcreteDataType::Binary(_) | ConcreteDataType::Json(_) => ScalarValue::Binary(None),
        ConcreteDataType::String(_) => ScalarValue::Utf8(None),
        ConcreteDataType::Date(_) => ScalarValue::Date32(None),
        ConcreteDataType::DateTime(_) => ScalarValue::Date64(None),
        ConcreteDataType::Timestamp(t) => timestamp_to_scalar_value(t.unit(), None),
        ConcreteDataType::Interval(v) => match v {
            IntervalType::YearMonth(_) => ScalarValue::IntervalYearMonth(None),
            IntervalType::DayTime(_) => ScalarValue::IntervalDayTime(None),
            IntervalType::MonthDayNano(_) => ScalarValue::IntervalMonthDayNano(None),
        },
        ConcreteDataType::List(_) => ScalarValue::List(Arc::new(ListArray::new_null(
            Arc::new(new_item_field(output_type.as_arrow_type())),
            0,
        ))),
        ConcreteDataType::Dictionary(dict) => ScalarValue::Dictionary(
            Box::new(dict.key_type().as_arrow_type()),
            Box::new(to_null_scalar_value(dict.value_type())?),
        ),
        ConcreteDataType::Time(t) => time_to_scalar_value(t.unit(), None)?,
        ConcreteDataType::Duration(d) => duration_to_scalar_value(d.unit(), None),
        ConcreteDataType::Decimal128(d) => ScalarValue::Decimal128(None, d.precision(), d.scale()),
    })
}

fn new_item_field(data_type: ArrowDataType) -> Field {
    Field::new("item", data_type, false)
}

pub fn timestamp_to_scalar_value(unit: TimeUnit, val: Option<i64>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(val, None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(val, None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(val, None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(val, None),
    }
}

/// Cast the 64-bit elapsed time into the arrow ScalarValue by time unit.
pub fn time_to_scalar_value(unit: TimeUnit, val: Option<i64>) -> Result<ScalarValue> {
    Ok(match unit {
        TimeUnit::Second => ScalarValue::Time32Second(
            val.map(|i| i.try_into().context(error::CastTimeTypeSnafu))
                .transpose()?,
        ),
        TimeUnit::Millisecond => ScalarValue::Time32Millisecond(
            val.map(|i| i.try_into().context(error::CastTimeTypeSnafu))
                .transpose()?,
        ),
        TimeUnit::Microsecond => ScalarValue::Time64Microsecond(val),
        TimeUnit::Nanosecond => ScalarValue::Time64Nanosecond(val),
    })
}

/// Cast the 64-bit duration into the arrow ScalarValue with time unit.
pub fn duration_to_scalar_value(unit: TimeUnit, val: Option<i64>) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::DurationSecond(val),
        TimeUnit::Millisecond => ScalarValue::DurationMillisecond(val),
        TimeUnit::Microsecond => ScalarValue::DurationMicrosecond(val),
        TimeUnit::Nanosecond => ScalarValue::DurationNanosecond(val),
    }
}

/// Convert [`ScalarValue`] to [`Timestamp`].
/// If it's `ScalarValue::Utf8`, try to parse it with the given timezone.
/// Return `None` if given scalar value cannot be converted to a valid timestamp.
pub fn scalar_value_to_timestamp(
    scalar: &ScalarValue,
    timezone: Option<&Timezone>,
) -> Option<Timestamp> {
    match scalar {
        ScalarValue::Utf8(Some(s)) => match Timestamp::from_str(s, timezone) {
            Ok(t) => Some(t),
            Err(e) => {
                error!(e;"Failed to convert string literal {s} to timestamp");
                None
            }
        },
        ScalarValue::TimestampSecond(v, _) => v.map(Timestamp::new_second),
        ScalarValue::TimestampMillisecond(v, _) => v.map(Timestamp::new_millisecond),
        ScalarValue::TimestampMicrosecond(v, _) => v.map(Timestamp::new_microsecond),
        ScalarValue::TimestampNanosecond(v, _) => v.map(Timestamp::new_nanosecond),
        _ => None,
    }
}

/// Convert [ScalarValue] to [Interval].
pub fn scalar_value_to_interval(scalar: &ScalarValue) -> Option<Interval> {
    match scalar {
        ScalarValue::IntervalYearMonth(v) => v.map(Interval::from_i32),
        ScalarValue::IntervalDayTime(v) => v.map(Interval::from_i64),
        ScalarValue::IntervalMonthDayNano(v) => v.map(Interval::from_i128),
        _ => None,
    }
}

macro_rules! impl_ord_for_value_like {
    ($Type: ident, $left: ident, $right: ident) => {
        if $left.is_null() && !$right.is_null() {
            return Ordering::Less;
        } else if !$left.is_null() && $right.is_null() {
            return Ordering::Greater;
        } else {
            match ($left, $right) {
                ($Type::Null, $Type::Null) => Ordering::Equal,
                ($Type::Boolean(v1), $Type::Boolean(v2)) => v1.cmp(v2),
                ($Type::UInt8(v1), $Type::UInt8(v2)) => v1.cmp(v2),
                ($Type::UInt16(v1), $Type::UInt16(v2)) => v1.cmp(v2),
                ($Type::UInt32(v1), $Type::UInt32(v2)) => v1.cmp(v2),
                ($Type::UInt64(v1), $Type::UInt64(v2)) => v1.cmp(v2),
                ($Type::Int8(v1), $Type::Int8(v2)) => v1.cmp(v2),
                ($Type::Int16(v1), $Type::Int16(v2)) => v1.cmp(v2),
                ($Type::Int32(v1), $Type::Int32(v2)) => v1.cmp(v2),
                ($Type::Int64(v1), $Type::Int64(v2)) => v1.cmp(v2),
                ($Type::Float32(v1), $Type::Float32(v2)) => v1.cmp(v2),
                ($Type::Float64(v1), $Type::Float64(v2)) => v1.cmp(v2),
                ($Type::String(v1), $Type::String(v2)) => v1.cmp(v2),
                ($Type::Binary(v1), $Type::Binary(v2)) => v1.cmp(v2),
                ($Type::Date(v1), $Type::Date(v2)) => v1.cmp(v2),
                ($Type::DateTime(v1), $Type::DateTime(v2)) => v1.cmp(v2),
                ($Type::Timestamp(v1), $Type::Timestamp(v2)) => v1.cmp(v2),
                ($Type::Time(v1), $Type::Time(v2)) => v1.cmp(v2),
                ($Type::Interval(v1), $Type::Interval(v2)) => v1.cmp(v2),
                ($Type::Duration(v1), $Type::Duration(v2)) => v1.cmp(v2),
                ($Type::List(v1), $Type::List(v2)) => v1.cmp(v2),
                _ => panic!(
                    "Cannot compare different values {:?} and {:?}",
                    $left, $right
                ),
            }
        }
    };
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        impl_ord_for_value_like!(Value, self, other)
    }
}

macro_rules! impl_try_from_value {
    ($Variant: ident, $Type: ident) => {
        impl TryFrom<Value> for $Type {
            type Error = Error;

            #[inline]
            fn try_from(from: Value) -> std::result::Result<Self, Self::Error> {
                match from {
                    Value::$Variant(v) => Ok(v.into()),
                    _ => TryFromValueSnafu {
                        reason: format!("{:?} is not a {}", from, stringify!($Type)),
                    }
                    .fail(),
                }
            }
        }

        impl TryFrom<Value> for Option<$Type> {
            type Error = Error;

            #[inline]
            fn try_from(from: Value) -> std::result::Result<Self, Self::Error> {
                match from {
                    Value::$Variant(v) => Ok(Some(v.into())),
                    Value::Null => Ok(None),
                    _ => TryFromValueSnafu {
                        reason: format!("{:?} is not a {}", from, stringify!($Type)),
                    }
                    .fail(),
                }
            }
        }
    };
}

impl_try_from_value!(Boolean, bool);
impl_try_from_value!(UInt8, u8);
impl_try_from_value!(UInt16, u16);
impl_try_from_value!(UInt32, u32);
impl_try_from_value!(UInt64, u64);
impl_try_from_value!(Int8, i8);
impl_try_from_value!(Int16, i16);
impl_try_from_value!(Int32, i32);
impl_try_from_value!(Int64, i64);
impl_try_from_value!(Float32, f32);
impl_try_from_value!(Float64, f64);
impl_try_from_value!(Float32, OrderedF32);
impl_try_from_value!(Float64, OrderedF64);
impl_try_from_value!(String, StringBytes);
impl_try_from_value!(Binary, Bytes);
impl_try_from_value!(Date, Date);
impl_try_from_value!(Time, Time);
impl_try_from_value!(DateTime, DateTime);
impl_try_from_value!(Timestamp, Timestamp);
impl_try_from_value!(Interval, Interval);
impl_try_from_value!(Duration, Duration);
impl_try_from_value!(Decimal128, Decimal128);

macro_rules! impl_value_from {
    ($Variant: ident, $Type: ident) => {
        impl From<$Type> for Value {
            fn from(value: $Type) -> Self {
                Value::$Variant(value.into())
            }
        }

        impl From<Option<$Type>> for Value {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => Value::$Variant(v.into()),
                    None => Value::Null,
                }
            }
        }
    };
}

impl_value_from!(Boolean, bool);
impl_value_from!(UInt8, u8);
impl_value_from!(UInt16, u16);
impl_value_from!(UInt32, u32);
impl_value_from!(UInt64, u64);
impl_value_from!(Int8, i8);
impl_value_from!(Int16, i16);
impl_value_from!(Int32, i32);
impl_value_from!(Int64, i64);
impl_value_from!(Float32, f32);
impl_value_from!(Float64, f64);
impl_value_from!(Float32, OrderedF32);
impl_value_from!(Float64, OrderedF64);
impl_value_from!(String, StringBytes);
impl_value_from!(Binary, Bytes);
impl_value_from!(Date, Date);
impl_value_from!(Time, Time);
impl_value_from!(DateTime, DateTime);
impl_value_from!(Timestamp, Timestamp);
impl_value_from!(Interval, Interval);
impl_value_from!(Duration, Duration);
impl_value_from!(String, String);
impl_value_from!(Decimal128, Decimal128);

impl From<&str> for Value {
    fn from(string: &str) -> Value {
        Value::String(string.into())
    }
}

impl From<Vec<u8>> for Value {
    fn from(bytes: Vec<u8>) -> Value {
        Value::Binary(bytes.into())
    }
}

impl From<&[u8]> for Value {
    fn from(bytes: &[u8]) -> Value {
        Value::Binary(bytes.into())
    }
}

impl TryFrom<Value> for serde_json::Value {
    type Error = serde_json::Error;

    fn try_from(value: Value) -> serde_json::Result<serde_json::Value> {
        let json_value = match value {
            Value::Null => serde_json::Value::Null,
            Value::Boolean(v) => serde_json::Value::Bool(v),
            Value::UInt8(v) => serde_json::Value::from(v),
            Value::UInt16(v) => serde_json::Value::from(v),
            Value::UInt32(v) => serde_json::Value::from(v),
            Value::UInt64(v) => serde_json::Value::from(v),
            Value::Int8(v) => serde_json::Value::from(v),
            Value::Int16(v) => serde_json::Value::from(v),
            Value::Int32(v) => serde_json::Value::from(v),
            Value::Int64(v) => serde_json::Value::from(v),
            Value::Float32(v) => serde_json::Value::from(v.0),
            Value::Float64(v) => serde_json::Value::from(v.0),
            Value::String(bytes) => serde_json::Value::String(bytes.into_string()),
            Value::Binary(bytes) => serde_json::to_value(bytes)?,
            Value::Date(v) => serde_json::Value::Number(v.val().into()),
            Value::DateTime(v) => serde_json::Value::Number(v.val().into()),
            Value::List(v) => serde_json::to_value(v)?,
            Value::Timestamp(v) => serde_json::to_value(v.value())?,
            Value::Time(v) => serde_json::to_value(v.value())?,
            Value::Interval(v) => serde_json::to_value(v.to_i128())?,
            Value::Duration(v) => serde_json::to_value(v.value())?,
            Value::Decimal128(v) => serde_json::to_value(v.to_string())?,
        };

        Ok(json_value)
    }
}

// TODO(yingwen): Consider removing the `datatype` field from `ListValue`.
/// List value.
#[derive(Debug, Clone, PartialEq, Hash, Serialize, Deserialize)]
pub struct ListValue {
    items: Vec<Value>,
    /// Inner values datatype, to distinguish empty lists of different datatypes.
    /// Restricted by DataFusion, cannot use null datatype for empty list.
    datatype: ConcreteDataType,
}

impl Eq for ListValue {}

impl ListValue {
    pub fn new(items: Vec<Value>, datatype: ConcreteDataType) -> Self {
        Self { items, datatype }
    }

    pub fn items(&self) -> &[Value] {
        &self.items
    }

    pub fn datatype(&self) -> &ConcreteDataType {
        &self.datatype
    }

    fn try_to_scalar_value(&self, output_type: &ListType) -> Result<ScalarValue> {
        let vs = self
            .items
            .iter()
            .map(|v| v.try_to_scalar_value(output_type.item_type()))
            .collect::<Result<Vec<_>>>()?;
        Ok(ScalarValue::List(ScalarValue::new_list(
            &vs,
            &self.datatype.as_arrow_type(),
        )))
    }

    /// use 'the first item size' * 'length of items' to estimate the size.
    /// it could be inaccurate.
    fn estimated_size(&self) -> usize {
        self.items
            .first()
            .map(|x| x.as_value_ref().data_size() * self.items.len())
            .unwrap_or(0)
    }
}

impl Default for ListValue {
    fn default() -> ListValue {
        ListValue::new(vec![], ConcreteDataType::null_datatype())
    }
}

impl PartialOrd for ListValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ListValue {
    fn cmp(&self, other: &Self) -> Ordering {
        assert_eq!(
            self.datatype, other.datatype,
            "Cannot compare different datatypes!"
        );
        self.items.cmp(&other.items)
    }
}

// TODO(ruihang): Implement this type
/// Dictionary value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DictionaryValue {
    /// Inner values datatypes
    key_type: ConcreteDataType,
    value_type: ConcreteDataType,
}

impl Eq for DictionaryValue {}

impl TryFrom<ScalarValue> for Value {
    type Error = error::Error;

    fn try_from(v: ScalarValue) -> Result<Self> {
        let v = match v {
            ScalarValue::Null => Value::Null,
            ScalarValue::Boolean(b) => Value::from(b),
            ScalarValue::Float32(f) => Value::from(f),
            ScalarValue::Float64(f) => Value::from(f),
            ScalarValue::Int8(i) => Value::from(i),
            ScalarValue::Int16(i) => Value::from(i),
            ScalarValue::Int32(i) => Value::from(i),
            ScalarValue::Int64(i) => Value::from(i),
            ScalarValue::UInt8(u) => Value::from(u),
            ScalarValue::UInt16(u) => Value::from(u),
            ScalarValue::UInt32(u) => Value::from(u),
            ScalarValue::UInt64(u) => Value::from(u),
            ScalarValue::Utf8(s) | ScalarValue::LargeUtf8(s) => {
                Value::from(s.map(StringBytes::from))
            }
            ScalarValue::Binary(b)
            | ScalarValue::LargeBinary(b)
            | ScalarValue::FixedSizeBinary(_, b) => Value::from(b.map(Bytes::from)),
            ScalarValue::List(array) => {
                let datatype = ConcreteDataType::try_from(array.data_type())?;
                let items = ScalarValue::convert_array_to_scalar_vec(array.as_ref())
                    .context(ConvertArrowArrayToScalarsSnafu)?
                    .into_iter()
                    .flatten()
                    .map(|x| x.try_into())
                    .collect::<Result<Vec<Value>>>()?;
                Value::List(ListValue::new(items, datatype))
            }
            ScalarValue::Date32(d) => d.map(|x| Value::Date(Date::new(x))).unwrap_or(Value::Null),
            ScalarValue::Date64(d) => d
                .map(|x| Value::DateTime(DateTime::new(x)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampSecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Second)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampMillisecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Millisecond)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampMicrosecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Microsecond)))
                .unwrap_or(Value::Null),
            ScalarValue::TimestampNanosecond(t, _) => t
                .map(|x| Value::Timestamp(Timestamp::new(x, TimeUnit::Nanosecond)))
                .unwrap_or(Value::Null),
            ScalarValue::Time32Second(t) => t
                .map(|x| Value::Time(Time::new(x as i64, TimeUnit::Second)))
                .unwrap_or(Value::Null),
            ScalarValue::Time32Millisecond(t) => t
                .map(|x| Value::Time(Time::new(x as i64, TimeUnit::Millisecond)))
                .unwrap_or(Value::Null),
            ScalarValue::Time64Microsecond(t) => t
                .map(|x| Value::Time(Time::new(x, TimeUnit::Microsecond)))
                .unwrap_or(Value::Null),
            ScalarValue::Time64Nanosecond(t) => t
                .map(|x| Value::Time(Time::new(x, TimeUnit::Nanosecond)))
                .unwrap_or(Value::Null),

            ScalarValue::IntervalYearMonth(t) => t
                .map(|x| Value::Interval(Interval::from_i32(x)))
                .unwrap_or(Value::Null),
            ScalarValue::IntervalDayTime(t) => t
                .map(|x| Value::Interval(Interval::from_i64(x)))
                .unwrap_or(Value::Null),
            ScalarValue::IntervalMonthDayNano(t) => t
                .map(|x| Value::Interval(Interval::from_i128(x)))
                .unwrap_or(Value::Null),
            ScalarValue::DurationSecond(d) => d
                .map(|x| Value::Duration(Duration::new(x, TimeUnit::Second)))
                .unwrap_or(Value::Null),
            ScalarValue::DurationMillisecond(d) => d
                .map(|x| Value::Duration(Duration::new(x, TimeUnit::Millisecond)))
                .unwrap_or(Value::Null),
            ScalarValue::DurationMicrosecond(d) => d
                .map(|x| Value::Duration(Duration::new(x, TimeUnit::Microsecond)))
                .unwrap_or(Value::Null),
            ScalarValue::DurationNanosecond(d) => d
                .map(|x| Value::Duration(Duration::new(x, TimeUnit::Nanosecond)))
                .unwrap_or(Value::Null),
            ScalarValue::Decimal128(v, p, s) => v
                .map(|v| Value::Decimal128(Decimal128::new(v, p, s)))
                .unwrap_or(Value::Null),
            ScalarValue::Decimal256(_, _, _)
            | ScalarValue::Struct(_)
            | ScalarValue::FixedSizeList(_)
            | ScalarValue::LargeList(_)
            | ScalarValue::Dictionary(_, _)
            | ScalarValue::Union(_, _, _)
            | ScalarValue::Float16(_) => {
                return error::UnsupportedArrowTypeSnafu {
                    arrow_type: v.data_type(),
                }
                .fail()
            }
        };
        Ok(v)
    }
}

impl From<ValueRef<'_>> for Value {
    fn from(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Value::Null,
            ValueRef::Boolean(v) => Value::Boolean(v),
            ValueRef::UInt8(v) => Value::UInt8(v),
            ValueRef::UInt16(v) => Value::UInt16(v),
            ValueRef::UInt32(v) => Value::UInt32(v),
            ValueRef::UInt64(v) => Value::UInt64(v),
            ValueRef::Int8(v) => Value::Int8(v),
            ValueRef::Int16(v) => Value::Int16(v),
            ValueRef::Int32(v) => Value::Int32(v),
            ValueRef::Int64(v) => Value::Int64(v),
            ValueRef::Float32(v) => Value::Float32(v),
            ValueRef::Float64(v) => Value::Float64(v),
            ValueRef::String(v) => Value::String(v.into()),
            ValueRef::Binary(v) => Value::Binary(v.into()),
            ValueRef::Date(v) => Value::Date(v),
            ValueRef::DateTime(v) => Value::DateTime(v),
            ValueRef::Timestamp(v) => Value::Timestamp(v),
            ValueRef::Time(v) => Value::Time(v),
            ValueRef::Interval(v) => Value::Interval(v),
            ValueRef::Duration(v) => Value::Duration(v),
            ValueRef::List(v) => v.to_value(),
            ValueRef::Decimal128(v) => Value::Decimal128(v),
        }
    }
}

/// Reference to [Value].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueRef<'a> {
    Null,

    // Numeric types:
    Boolean(bool),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float32(OrderedF32),
    Float64(OrderedF64),

    // Decimal type:
    Decimal128(Decimal128),

    // String types:
    String(&'a str),
    Binary(&'a [u8]),

    // Date & Time types:
    Date(Date),
    DateTime(DateTime),
    Timestamp(Timestamp),
    Time(Time),
    Duration(Duration),
    Interval(Interval),

    // Compound types:
    List(ListValueRef<'a>),
}

macro_rules! impl_as_for_value_ref {
    ($value: ident, $Variant: ident) => {
        match $value {
            ValueRef::Null => Ok(None),
            ValueRef::$Variant(v) => Ok(Some(*v)),
            other => error::CastTypeSnafu {
                msg: format!(
                    "Failed to cast value ref {:?} to {}",
                    other,
                    stringify!($Variant)
                ),
            }
            .fail(),
        }
    };
}

impl<'a> ValueRef<'a> {
    define_data_type_func!(ValueRef);

    /// Returns true if this is null.
    pub fn is_null(&self) -> bool {
        matches!(self, ValueRef::Null)
    }

    /// Cast itself to binary slice.
    pub fn as_binary(&self) -> Result<Option<&[u8]>> {
        impl_as_for_value_ref!(self, Binary)
    }

    /// Cast itself to string slice.
    pub fn as_string(&self) -> Result<Option<&str>> {
        impl_as_for_value_ref!(self, String)
    }

    /// Cast itself to boolean.
    pub fn as_boolean(&self) -> Result<Option<bool>> {
        impl_as_for_value_ref!(self, Boolean)
    }

    pub fn as_i8(&self) -> Result<Option<i8>> {
        impl_as_for_value_ref!(self, Int8)
    }

    pub fn as_u8(&self) -> Result<Option<u8>> {
        impl_as_for_value_ref!(self, UInt8)
    }

    pub fn as_i16(&self) -> Result<Option<i16>> {
        impl_as_for_value_ref!(self, Int16)
    }

    pub fn as_u16(&self) -> Result<Option<u16>> {
        impl_as_for_value_ref!(self, UInt16)
    }

    pub fn as_i32(&self) -> Result<Option<i32>> {
        impl_as_for_value_ref!(self, Int32)
    }

    pub fn as_u32(&self) -> Result<Option<u32>> {
        impl_as_for_value_ref!(self, UInt32)
    }

    pub fn as_i64(&self) -> Result<Option<i64>> {
        impl_as_for_value_ref!(self, Int64)
    }

    pub fn as_u64(&self) -> Result<Option<u64>> {
        impl_as_for_value_ref!(self, UInt64)
    }

    pub fn as_f32(&self) -> Result<Option<f32>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::Float32(f) => Ok(Some(f.0)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to ValueRef::Float32", other,),
            }
            .fail(),
        }
    }

    pub fn as_f64(&self) -> Result<Option<f64>> {
        match self {
            ValueRef::Null => Ok(None),
            ValueRef::Float64(f) => Ok(Some(f.0)),
            other => error::CastTypeSnafu {
                msg: format!("Failed to cast value ref {:?} to ValueRef::Float64", other,),
            }
            .fail(),
        }
    }

    /// Cast itself to [Date].
    pub fn as_date(&self) -> Result<Option<Date>> {
        impl_as_for_value_ref!(self, Date)
    }

    /// Cast itself to [DateTime].
    pub fn as_datetime(&self) -> Result<Option<DateTime>> {
        impl_as_for_value_ref!(self, DateTime)
    }

    /// Cast itself to [Timestamp].
    pub fn as_timestamp(&self) -> Result<Option<Timestamp>> {
        impl_as_for_value_ref!(self, Timestamp)
    }

    /// Cast itself to [Time].
    pub fn as_time(&self) -> Result<Option<Time>> {
        impl_as_for_value_ref!(self, Time)
    }

    pub fn as_duration(&self) -> Result<Option<Duration>> {
        impl_as_for_value_ref!(self, Duration)
    }

    /// Cast itself to [Interval].
    pub fn as_interval(&self) -> Result<Option<Interval>> {
        impl_as_for_value_ref!(self, Interval)
    }

    /// Cast itself to [ListValueRef].
    pub fn as_list(&self) -> Result<Option<ListValueRef>> {
        impl_as_for_value_ref!(self, List)
    }

    /// Cast itself to [Decimal128].
    pub fn as_decimal128(&self) -> Result<Option<Decimal128>> {
        impl_as_for_value_ref!(self, Decimal128)
    }
}

impl<'a> PartialOrd for ValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> Ord for ValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        impl_ord_for_value_like!(ValueRef, self, other)
    }
}

macro_rules! impl_value_ref_from {
    ($Variant:ident, $Type:ident) => {
        impl From<$Type> for ValueRef<'_> {
            fn from(value: $Type) -> Self {
                ValueRef::$Variant(value.into())
            }
        }

        impl From<Option<$Type>> for ValueRef<'_> {
            fn from(value: Option<$Type>) -> Self {
                match value {
                    Some(v) => ValueRef::$Variant(v.into()),
                    None => ValueRef::Null,
                }
            }
        }
    };
}

impl_value_ref_from!(Boolean, bool);
impl_value_ref_from!(UInt8, u8);
impl_value_ref_from!(UInt16, u16);
impl_value_ref_from!(UInt32, u32);
impl_value_ref_from!(UInt64, u64);
impl_value_ref_from!(Int8, i8);
impl_value_ref_from!(Int16, i16);
impl_value_ref_from!(Int32, i32);
impl_value_ref_from!(Int64, i64);
impl_value_ref_from!(Float32, f32);
impl_value_ref_from!(Float64, f64);
impl_value_ref_from!(Date, Date);
impl_value_ref_from!(DateTime, DateTime);
impl_value_ref_from!(Timestamp, Timestamp);
impl_value_ref_from!(Time, Time);
impl_value_ref_from!(Interval, Interval);
impl_value_ref_from!(Duration, Duration);
impl_value_ref_from!(Decimal128, Decimal128);

impl<'a> From<&'a str> for ValueRef<'a> {
    fn from(string: &'a str) -> ValueRef<'a> {
        ValueRef::String(string)
    }
}

impl<'a> From<&'a [u8]> for ValueRef<'a> {
    fn from(bytes: &'a [u8]) -> ValueRef<'a> {
        ValueRef::Binary(bytes)
    }
}

impl<'a> From<Option<ListValueRef<'a>>> for ValueRef<'a> {
    fn from(list: Option<ListValueRef>) -> ValueRef {
        match list {
            Some(v) => ValueRef::List(v),
            None => ValueRef::Null,
        }
    }
}

pub struct ColumnPair<'a> {
    pub value: ValueRef<'a>,
    pub schema: &'a ColumnSchema,
}

impl<'a> TryFrom<ColumnPair<'a>> for serde_json::Value {
    type Error = serde_json::Error;

    fn try_from(value: ColumnPair<'a>) -> serde_json::Result<serde_json::Value> {
        let ColumnPair { value, schema } = value;
        let json_value = match value {
            ValueRef::Null => serde_json::Value::Null,
            ValueRef::Boolean(v) => serde_json::Value::Bool(v),
            ValueRef::UInt8(v) => serde_json::Value::from(v),
            ValueRef::UInt16(v) => serde_json::Value::from(v),
            ValueRef::UInt32(v) => serde_json::Value::from(v),
            ValueRef::UInt64(v) => serde_json::Value::from(v),
            ValueRef::Int8(v) => serde_json::Value::from(v),
            ValueRef::Int16(v) => serde_json::Value::from(v),
            ValueRef::Int32(v) => serde_json::Value::from(v),
            ValueRef::Int64(v) => serde_json::Value::from(v),
            ValueRef::Float32(v) => serde_json::Value::from(v.0),
            ValueRef::Float64(v) => serde_json::Value::from(v.0),
            ValueRef::String(bytes) => serde_json::Value::String(bytes.to_string()),
            ValueRef::Binary(bytes) => {
                if let ConcreteDataType::Json(_) = schema.data_type {
                    match jsonb::from_slice(bytes) {
                        Ok(json) => json.into(),
                        Err(e) => {
                            error!(e; "Failed to parse jsonb");
                            serde_json::Value::Null
                        }
                    }
                } else {
                    serde_json::to_value(bytes)?
                }
            }
            ValueRef::Date(v) => serde_json::Value::Number(v.val().into()),
            ValueRef::DateTime(v) => serde_json::Value::Number(v.val().into()),
            ValueRef::List(v) => serde_json::to_value(v)?,
            ValueRef::Timestamp(v) => serde_json::to_value(v.value())?,
            ValueRef::Time(v) => serde_json::to_value(v.value())?,
            ValueRef::Interval(v) => serde_json::to_value(v.to_i128())?,
            ValueRef::Duration(v) => serde_json::to_value(v.value())?,
            ValueRef::Decimal128(v) => serde_json::to_value(v.to_string())?,
        };

        Ok(json_value)
    }
}

/// Reference to a [ListValue].
///
/// Now comparison still requires some allocation (call of `to_value()`) and
/// might be avoidable by downcasting and comparing the underlying array slice
/// if it becomes bottleneck.
#[derive(Debug, Clone, Copy)]
pub enum ListValueRef<'a> {
    // TODO(yingwen): Consider replace this by VectorRef.
    Indexed { vector: &'a ListVector, idx: usize },
    Ref { val: &'a ListValue },
}

impl<'a> ListValueRef<'a> {
    /// Convert self to [Value]. This method would clone the underlying data.
    fn to_value(self) -> Value {
        match self {
            ListValueRef::Indexed { vector, idx } => vector.get(idx),
            ListValueRef::Ref { val } => Value::List(val.clone()),
        }
    }

    /// Returns the inner element's data type.
    fn datatype(&self) -> ConcreteDataType {
        match self {
            ListValueRef::Indexed { vector, .. } => vector.data_type(),
            ListValueRef::Ref { val } => val.datatype().clone(),
        }
    }
}

impl<'a> Serialize for ListValueRef<'a> {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        match self {
            ListValueRef::Indexed { vector, idx } => match vector.get(*idx) {
                Value::List(v) => v.serialize(serializer),
                _ => unreachable!(),
            },
            ListValueRef::Ref { val } => val.serialize(serializer),
        }
    }
}

impl<'a> PartialEq for ListValueRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.to_value().eq(&other.to_value())
    }
}

impl<'a> Eq for ListValueRef<'a> {}

impl<'a> Ord for ListValueRef<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // Respect the order of `Value` by converting into value before comparison.
        self.to_value().cmp(&other.to_value())
    }
}

impl<'a> PartialOrd for ListValueRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> ValueRef<'a> {
    /// Returns the size of the underlying data in bytes,
    /// The size is estimated and only considers the data size.
    pub fn data_size(&self) -> usize {
        match *self {
            // Since the `Null` type is also considered to occupy space, we have opted to use the
            // size of `i64` as an initial approximation.
            ValueRef::Null => 8,
            ValueRef::Boolean(_) => 1,
            ValueRef::UInt8(_) => 1,
            ValueRef::UInt16(_) => 2,
            ValueRef::UInt32(_) => 4,
            ValueRef::UInt64(_) => 8,
            ValueRef::Int8(_) => 1,
            ValueRef::Int16(_) => 2,
            ValueRef::Int32(_) => 4,
            ValueRef::Int64(_) => 8,
            ValueRef::Float32(_) => 4,
            ValueRef::Float64(_) => 8,
            ValueRef::String(v) => std::mem::size_of_val(v),
            ValueRef::Binary(v) => std::mem::size_of_val(v),
            ValueRef::Date(_) => 4,
            ValueRef::DateTime(_) => 8,
            ValueRef::Timestamp(_) => 16,
            ValueRef::Time(_) => 16,
            ValueRef::Duration(_) => 16,
            ValueRef::Interval(_) => 24,
            ValueRef::Decimal128(_) => 32,
            ValueRef::List(v) => match v {
                ListValueRef::Indexed { vector, .. } => vector.memory_size() / vector.len(),
                ListValueRef::Ref { val } => val.estimated_size(),
            },
        }
    }
}

pub fn column_data_to_json(data: ValueData) -> JsonValue {
    match data {
        ValueData::BinaryValue(b) => JsonValue::String(URL_SAFE.encode(b)),
        ValueData::BoolValue(b) => JsonValue::Bool(b),
        ValueData::U8Value(i) => JsonValue::Number(i.into()),
        ValueData::U16Value(i) => JsonValue::Number(i.into()),
        ValueData::U32Value(i) => JsonValue::Number(i.into()),
        ValueData::U64Value(i) => JsonValue::Number(i.into()),
        ValueData::I8Value(i) => JsonValue::Number(i.into()),
        ValueData::I16Value(i) => JsonValue::Number(i.into()),
        ValueData::I32Value(i) => JsonValue::Number(i.into()),
        ValueData::I64Value(i) => JsonValue::Number(i.into()),
        ValueData::F32Value(f) => Number::from_f64(f as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ValueData::F64Value(f) => Number::from_f64(f)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ValueData::StringValue(s) => JsonValue::String(s),
        ValueData::DateValue(d) => JsonValue::String(Date::from(d).to_string()),
        ValueData::DatetimeValue(d) => JsonValue::String(DateTime::from(d).to_string()),
        ValueData::TimeSecondValue(d) => JsonValue::String(Time::new_second(d).to_iso8601_string()),
        ValueData::TimeMillisecondValue(d) => {
            JsonValue::String(Time::new_millisecond(d).to_iso8601_string())
        }
        ValueData::TimeMicrosecondValue(d) => {
            JsonValue::String(Time::new_microsecond(d).to_iso8601_string())
        }
        ValueData::TimeNanosecondValue(d) => {
            JsonValue::String(Time::new_nanosecond(d).to_iso8601_string())
        }
        ValueData::TimestampMicrosecondValue(d) => {
            JsonValue::String(Timestamp::new_microsecond(d).to_iso8601_string())
        }
        ValueData::TimestampMillisecondValue(d) => {
            JsonValue::String(Timestamp::new_millisecond(d).to_iso8601_string())
        }
        ValueData::TimestampNanosecondValue(d) => {
            JsonValue::String(Timestamp::new_nanosecond(d).to_iso8601_string())
        }
        ValueData::TimestampSecondValue(d) => {
            JsonValue::String(Timestamp::new_second(d).to_iso8601_string())
        }
        ValueData::IntervalYearMonthValue(d) => JsonValue::String(format!("interval year [{}]", d)),
        ValueData::IntervalMonthDayNanoValue(d) => JsonValue::String(format!(
            "interval month [{}][{}][{}]",
            d.months, d.days, d.nanoseconds
        )),
        ValueData::IntervalDayTimeValue(d) => JsonValue::String(format!("interval day [{}]", d)),
        ValueData::Decimal128Value(d) => {
            JsonValue::String(format!("decimal128 [{}][{}]", d.hi, d.lo))
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::DataType as ArrowDataType;
    use common_time::timezone::set_default_timezone;
    use greptime_proto::v1::{Decimal128 as ProtoDecimal128, IntervalMonthDayNano};
    use num_traits::Float;

    use super::*;
    use crate::vectors::ListVectorBuilder;

    #[test]
    fn test_column_data_to_json() {
        assert_eq!(
            column_data_to_json(ValueData::BinaryValue(b"hello".to_vec())),
            JsonValue::String("aGVsbG8=".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::BoolValue(true)),
            JsonValue::Bool(true)
        );
        assert_eq!(
            column_data_to_json(ValueData::U8Value(1)),
            JsonValue::Number(1.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::U16Value(2)),
            JsonValue::Number(2.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::U32Value(3)),
            JsonValue::Number(3.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::U64Value(4)),
            JsonValue::Number(4.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::I8Value(5)),
            JsonValue::Number(5.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::I16Value(6)),
            JsonValue::Number(6.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::I32Value(7)),
            JsonValue::Number(7.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::I64Value(8)),
            JsonValue::Number(8.into())
        );
        assert_eq!(
            column_data_to_json(ValueData::F32Value(9.0)),
            JsonValue::Number(Number::from_f64(9.0_f64).unwrap())
        );
        assert_eq!(
            column_data_to_json(ValueData::F64Value(10.0)),
            JsonValue::Number(Number::from_f64(10.0_f64).unwrap())
        );
        assert_eq!(
            column_data_to_json(ValueData::StringValue("hello".to_string())),
            JsonValue::String("hello".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::DateValue(123)),
            JsonValue::String("1970-05-04".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::DatetimeValue(456)),
            JsonValue::String("1970-01-01 00:00:00.456+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::TimeSecondValue(789)),
            JsonValue::String("00:13:09+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::TimeMillisecondValue(789)),
            JsonValue::String("00:00:00.789+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::TimeMicrosecondValue(789)),
            JsonValue::String("00:00:00.000789+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::TimestampMillisecondValue(1234567890)),
            JsonValue::String("1970-01-15 06:56:07.890+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::TimestampNanosecondValue(1234567890123456789)),
            JsonValue::String("2009-02-13 23:31:30.123456789+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::TimestampSecondValue(1234567890)),
            JsonValue::String("2009-02-13 23:31:30+0000".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::IntervalYearMonthValue(12)),
            JsonValue::String("interval year [12]".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::IntervalMonthDayNanoValue(IntervalMonthDayNano {
                months: 1,
                days: 2,
                nanoseconds: 3,
            })),
            JsonValue::String("interval month [1][2][3]".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::IntervalDayTimeValue(4)),
            JsonValue::String("interval day [4]".to_string())
        );
        assert_eq!(
            column_data_to_json(ValueData::Decimal128Value(ProtoDecimal128 { hi: 5, lo: 6 })),
            JsonValue::String("decimal128 [5][6]".to_string())
        );
    }

    #[test]
    fn test_try_from_scalar_value() {
        assert_eq!(
            Value::Boolean(true),
            ScalarValue::Boolean(Some(true)).try_into().unwrap()
        );
        assert_eq!(
            Value::Boolean(false),
            ScalarValue::Boolean(Some(false)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Boolean(None).try_into().unwrap());

        assert_eq!(
            Value::Float32(1.0f32.into()),
            ScalarValue::Float32(Some(1.0f32)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Float32(None).try_into().unwrap());

        assert_eq!(
            Value::Float64(2.0f64.into()),
            ScalarValue::Float64(Some(2.0f64)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Float64(None).try_into().unwrap());

        assert_eq!(
            Value::Int8(i8::MAX),
            ScalarValue::Int8(Some(i8::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Int8(None).try_into().unwrap());

        assert_eq!(
            Value::Int16(i16::MAX),
            ScalarValue::Int16(Some(i16::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Int16(None).try_into().unwrap());

        assert_eq!(
            Value::Int32(i32::MAX),
            ScalarValue::Int32(Some(i32::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Int32(None).try_into().unwrap());

        assert_eq!(
            Value::Int64(i64::MAX),
            ScalarValue::Int64(Some(i64::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Int64(None).try_into().unwrap());

        assert_eq!(
            Value::UInt8(u8::MAX),
            ScalarValue::UInt8(Some(u8::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::UInt8(None).try_into().unwrap());

        assert_eq!(
            Value::UInt16(u16::MAX),
            ScalarValue::UInt16(Some(u16::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::UInt16(None).try_into().unwrap());

        assert_eq!(
            Value::UInt32(u32::MAX),
            ScalarValue::UInt32(Some(u32::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::UInt32(None).try_into().unwrap());

        assert_eq!(
            Value::UInt64(u64::MAX),
            ScalarValue::UInt64(Some(u64::MAX)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::UInt64(None).try_into().unwrap());

        assert_eq!(
            Value::from("hello"),
            ScalarValue::Utf8(Some("hello".to_string()))
                .try_into()
                .unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Utf8(None).try_into().unwrap());

        assert_eq!(
            Value::from("large_hello"),
            ScalarValue::LargeUtf8(Some("large_hello".to_string()))
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::LargeUtf8(None).try_into().unwrap()
        );

        assert_eq!(
            Value::from("world".as_bytes()),
            ScalarValue::Binary(Some("world".as_bytes().to_vec()))
                .try_into()
                .unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Binary(None).try_into().unwrap());

        assert_eq!(
            Value::from("large_world".as_bytes()),
            ScalarValue::LargeBinary(Some("large_world".as_bytes().to_vec()))
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::LargeBinary(None).try_into().unwrap()
        );

        assert_eq!(
            Value::List(ListValue::new(
                vec![Value::Int32(1), Value::Null],
                ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype())
            )),
            ScalarValue::List(ScalarValue::new_list(
                &[ScalarValue::Int32(Some(1)), ScalarValue::Int32(None)],
                &ArrowDataType::Int32,
            ))
            .try_into()
            .unwrap()
        );
        assert_eq!(
            Value::List(ListValue::new(
                vec![],
                ConcreteDataType::list_datatype(ConcreteDataType::uint32_datatype())
            )),
            ScalarValue::List(ScalarValue::new_list(&[], &ArrowDataType::UInt32))
                .try_into()
                .unwrap()
        );

        assert_eq!(
            Value::Date(Date::new(123)),
            ScalarValue::Date32(Some(123)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Date32(None).try_into().unwrap());

        assert_eq!(
            Value::DateTime(DateTime::new(456)),
            ScalarValue::Date64(Some(456)).try_into().unwrap()
        );
        assert_eq!(Value::Null, ScalarValue::Date64(None).try_into().unwrap());

        assert_eq!(
            Value::Timestamp(Timestamp::new(1, TimeUnit::Second)),
            ScalarValue::TimestampSecond(Some(1), None)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::TimestampSecond(None, None).try_into().unwrap()
        );

        assert_eq!(
            Value::Timestamp(Timestamp::new(1, TimeUnit::Millisecond)),
            ScalarValue::TimestampMillisecond(Some(1), None)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::TimestampMillisecond(None, None)
                .try_into()
                .unwrap()
        );

        assert_eq!(
            Value::Timestamp(Timestamp::new(1, TimeUnit::Microsecond)),
            ScalarValue::TimestampMicrosecond(Some(1), None)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::TimestampMicrosecond(None, None)
                .try_into()
                .unwrap()
        );

        assert_eq!(
            Value::Timestamp(Timestamp::new(1, TimeUnit::Nanosecond)),
            ScalarValue::TimestampNanosecond(Some(1), None)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::TimestampNanosecond(None, None)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::IntervalMonthDayNano(None).try_into().unwrap()
        );
        assert_eq!(
            Value::Interval(Interval::from_month_day_nano(1, 1, 1)),
            ScalarValue::IntervalMonthDayNano(Some(
                Interval::from_month_day_nano(1, 1, 1).to_i128()
            ))
            .try_into()
            .unwrap()
        );

        assert_eq!(
            Value::Time(Time::new(1, TimeUnit::Second)),
            ScalarValue::Time32Second(Some(1)).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::Time32Second(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Time(Time::new(1, TimeUnit::Millisecond)),
            ScalarValue::Time32Millisecond(Some(1)).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::Time32Millisecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Time(Time::new(1, TimeUnit::Microsecond)),
            ScalarValue::Time64Microsecond(Some(1)).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::Time64Microsecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Time(Time::new(1, TimeUnit::Nanosecond)),
            ScalarValue::Time64Nanosecond(Some(1)).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::Time64Nanosecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Duration(Duration::new_second(1)),
            ScalarValue::DurationSecond(Some(1)).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::DurationSecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Duration(Duration::new_millisecond(1)),
            ScalarValue::DurationMillisecond(Some(1))
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::DurationMillisecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Duration(Duration::new_microsecond(1)),
            ScalarValue::DurationMicrosecond(Some(1))
                .try_into()
                .unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::DurationMicrosecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Duration(Duration::new_nanosecond(1)),
            ScalarValue::DurationNanosecond(Some(1)).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::DurationNanosecond(None).try_into().unwrap()
        );

        assert_eq!(
            Value::Decimal128(Decimal128::new(1, 38, 10)),
            ScalarValue::Decimal128(Some(1), 38, 10).try_into().unwrap()
        );
        assert_eq!(
            Value::Null,
            ScalarValue::Decimal128(None, 0, 0).try_into().unwrap()
        );
    }

    #[test]
    fn test_value_from_inner() {
        assert_eq!(Value::Boolean(true), Value::from(true));
        assert_eq!(Value::Boolean(false), Value::from(false));

        assert_eq!(Value::UInt8(u8::MIN), Value::from(u8::MIN));
        assert_eq!(Value::UInt8(u8::MAX), Value::from(u8::MAX));

        assert_eq!(Value::UInt16(u16::MIN), Value::from(u16::MIN));
        assert_eq!(Value::UInt16(u16::MAX), Value::from(u16::MAX));

        assert_eq!(Value::UInt32(u32::MIN), Value::from(u32::MIN));
        assert_eq!(Value::UInt32(u32::MAX), Value::from(u32::MAX));

        assert_eq!(Value::UInt64(u64::MIN), Value::from(u64::MIN));
        assert_eq!(Value::UInt64(u64::MAX), Value::from(u64::MAX));

        assert_eq!(Value::Int8(i8::MIN), Value::from(i8::MIN));
        assert_eq!(Value::Int8(i8::MAX), Value::from(i8::MAX));

        assert_eq!(Value::Int16(i16::MIN), Value::from(i16::MIN));
        assert_eq!(Value::Int16(i16::MAX), Value::from(i16::MAX));

        assert_eq!(Value::Int32(i32::MIN), Value::from(i32::MIN));
        assert_eq!(Value::Int32(i32::MAX), Value::from(i32::MAX));

        assert_eq!(Value::Int64(i64::MIN), Value::from(i64::MIN));
        assert_eq!(Value::Int64(i64::MAX), Value::from(i64::MAX));

        assert_eq!(
            Value::Float32(OrderedFloat(f32::MIN)),
            Value::from(f32::MIN)
        );
        assert_eq!(
            Value::Float32(OrderedFloat(f32::MAX)),
            Value::from(f32::MAX)
        );

        assert_eq!(
            Value::Float64(OrderedFloat(f64::MIN)),
            Value::from(f64::MIN)
        );
        assert_eq!(
            Value::Float64(OrderedFloat(f64::MAX)),
            Value::from(f64::MAX)
        );

        let string_bytes = StringBytes::from("hello");
        assert_eq!(
            Value::String(string_bytes.clone()),
            Value::from(string_bytes)
        );

        let bytes = Bytes::from(b"world".as_slice());
        assert_eq!(Value::Binary(bytes.clone()), Value::from(bytes));
    }

    fn check_type_and_value(data_type: &ConcreteDataType, value: &Value) {
        assert_eq!(*data_type, value.data_type());
        assert_eq!(data_type.logical_type_id(), value.logical_type_id());
    }

    #[test]
    fn test_value_datatype() {
        check_type_and_value(&ConcreteDataType::boolean_datatype(), &Value::Boolean(true));
        check_type_and_value(&ConcreteDataType::uint8_datatype(), &Value::UInt8(u8::MIN));
        check_type_and_value(
            &ConcreteDataType::uint16_datatype(),
            &Value::UInt16(u16::MIN),
        );
        check_type_and_value(
            &ConcreteDataType::uint16_datatype(),
            &Value::UInt16(u16::MAX),
        );
        check_type_and_value(
            &ConcreteDataType::uint32_datatype(),
            &Value::UInt32(u32::MIN),
        );
        check_type_and_value(
            &ConcreteDataType::uint64_datatype(),
            &Value::UInt64(u64::MIN),
        );
        check_type_and_value(&ConcreteDataType::int8_datatype(), &Value::Int8(i8::MIN));
        check_type_and_value(&ConcreteDataType::int16_datatype(), &Value::Int16(i16::MIN));
        check_type_and_value(&ConcreteDataType::int32_datatype(), &Value::Int32(i32::MIN));
        check_type_and_value(&ConcreteDataType::int64_datatype(), &Value::Int64(i64::MIN));
        check_type_and_value(
            &ConcreteDataType::float32_datatype(),
            &Value::Float32(OrderedFloat(f32::MIN)),
        );
        check_type_and_value(
            &ConcreteDataType::float64_datatype(),
            &Value::Float64(OrderedFloat(f64::MIN)),
        );
        check_type_and_value(
            &ConcreteDataType::string_datatype(),
            &Value::String(StringBytes::from("hello")),
        );
        check_type_and_value(
            &ConcreteDataType::binary_datatype(),
            &Value::Binary(Bytes::from(b"world".as_slice())),
        );
        check_type_and_value(
            &ConcreteDataType::list_datatype(ConcreteDataType::int32_datatype()),
            &Value::List(ListValue::new(
                vec![Value::Int32(10)],
                ConcreteDataType::int32_datatype(),
            )),
        );
        check_type_and_value(
            &ConcreteDataType::list_datatype(ConcreteDataType::null_datatype()),
            &Value::List(ListValue::default()),
        );
        check_type_and_value(
            &ConcreteDataType::date_datatype(),
            &Value::Date(Date::new(1)),
        );
        check_type_and_value(
            &ConcreteDataType::datetime_datatype(),
            &Value::DateTime(DateTime::new(1)),
        );
        check_type_and_value(
            &ConcreteDataType::timestamp_millisecond_datatype(),
            &Value::Timestamp(Timestamp::new_millisecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::time_second_datatype(),
            &Value::Time(Time::new_second(1)),
        );
        check_type_and_value(
            &ConcreteDataType::time_millisecond_datatype(),
            &Value::Time(Time::new_millisecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::time_microsecond_datatype(),
            &Value::Time(Time::new_microsecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::time_nanosecond_datatype(),
            &Value::Time(Time::new_nanosecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::interval_month_day_nano_datatype(),
            &Value::Interval(Interval::from_month_day_nano(1, 2, 3)),
        );
        check_type_and_value(
            &ConcreteDataType::duration_second_datatype(),
            &Value::Duration(Duration::new_second(1)),
        );
        check_type_and_value(
            &ConcreteDataType::duration_millisecond_datatype(),
            &Value::Duration(Duration::new_millisecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::duration_microsecond_datatype(),
            &Value::Duration(Duration::new_microsecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::duration_nanosecond_datatype(),
            &Value::Duration(Duration::new_nanosecond(1)),
        );
        check_type_and_value(
            &ConcreteDataType::decimal128_datatype(38, 10),
            &Value::Decimal128(Decimal128::new(1, 38, 10)),
        );
    }

    #[test]
    fn test_value_from_string() {
        let hello = "hello".to_string();
        assert_eq!(
            Value::String(StringBytes::from(hello.clone())),
            Value::from(hello)
        );

        let world = "world";
        assert_eq!(Value::String(StringBytes::from(world)), Value::from(world));
    }

    #[test]
    fn test_value_from_bytes() {
        let hello = b"hello".to_vec();
        assert_eq!(
            Value::Binary(Bytes::from(hello.clone())),
            Value::from(hello)
        );

        let world: &[u8] = b"world";
        assert_eq!(Value::Binary(Bytes::from(world)), Value::from(world));
    }

    fn to_json(value: Value) -> serde_json::Value {
        value.try_into().unwrap()
    }

    #[test]
    fn test_to_json_value() {
        assert_eq!(serde_json::Value::Null, to_json(Value::Null));
        assert_eq!(serde_json::Value::Bool(true), to_json(Value::Boolean(true)));
        assert_eq!(
            serde_json::Value::Number(20u8.into()),
            to_json(Value::UInt8(20))
        );
        assert_eq!(
            serde_json::Value::Number(20i8.into()),
            to_json(Value::Int8(20))
        );
        assert_eq!(
            serde_json::Value::Number(2000u16.into()),
            to_json(Value::UInt16(2000))
        );
        assert_eq!(
            serde_json::Value::Number(2000i16.into()),
            to_json(Value::Int16(2000))
        );
        assert_eq!(
            serde_json::Value::Number(3000u32.into()),
            to_json(Value::UInt32(3000))
        );
        assert_eq!(
            serde_json::Value::Number(3000i32.into()),
            to_json(Value::Int32(3000))
        );
        assert_eq!(
            serde_json::Value::Number(4000u64.into()),
            to_json(Value::UInt64(4000))
        );
        assert_eq!(
            serde_json::Value::Number(4000i64.into()),
            to_json(Value::Int64(4000))
        );
        assert_eq!(
            serde_json::Value::from(125.0f32),
            to_json(Value::Float32(125.0.into()))
        );
        assert_eq!(
            serde_json::Value::from(125.0f64),
            to_json(Value::Float64(125.0.into()))
        );
        assert_eq!(
            serde_json::Value::String(String::from("hello")),
            to_json(Value::String(StringBytes::from("hello")))
        );
        assert_eq!(
            serde_json::Value::from(b"world".as_slice()),
            to_json(Value::Binary(Bytes::from(b"world".as_slice())))
        );
        assert_eq!(
            serde_json::Value::Number(5000i32.into()),
            to_json(Value::Date(Date::new(5000)))
        );
        assert_eq!(
            serde_json::Value::Number(5000i64.into()),
            to_json(Value::DateTime(DateTime::new(5000)))
        );

        assert_eq!(
            serde_json::Value::Number(1.into()),
            to_json(Value::Timestamp(Timestamp::new_millisecond(1)))
        );
        assert_eq!(
            serde_json::Value::Number(1.into()),
            to_json(Value::Time(Time::new_millisecond(1)))
        );
        assert_eq!(
            serde_json::Value::Number(1.into()),
            to_json(Value::Duration(Duration::new_millisecond(1)))
        );

        let json_value: serde_json::Value =
            serde_json::from_str(r#"{"items":[{"Int32":123}],"datatype":{"Int32":{}}}"#).unwrap();
        assert_eq!(
            json_value,
            to_json(Value::List(ListValue {
                items: vec![Value::Int32(123)],
                datatype: ConcreteDataType::int32_datatype(),
            }))
        );
    }

    #[test]
    fn test_null_value() {
        assert!(Value::Null.is_null());
        assert!(!Value::Boolean(true).is_null());
        assert!(Value::Null < Value::Boolean(false));
        assert!(Value::Boolean(true) > Value::Null);
        assert!(Value::Null < Value::Int32(10));
        assert!(Value::Int32(10) > Value::Null);
    }

    #[test]
    fn test_null_value_ref() {
        assert!(ValueRef::Null.is_null());
        assert!(!ValueRef::Boolean(true).is_null());
        assert!(ValueRef::Null < ValueRef::Boolean(false));
        assert!(ValueRef::Boolean(true) > ValueRef::Null);
        assert!(ValueRef::Null < ValueRef::Int32(10));
        assert!(ValueRef::Int32(10) > ValueRef::Null);
    }

    #[test]
    fn test_as_value_ref() {
        macro_rules! check_as_value_ref {
            ($Variant: ident, $data: expr) => {
                let value = Value::$Variant($data);
                let value_ref = value.as_value_ref();
                let expect_ref = ValueRef::$Variant($data);

                assert_eq!(expect_ref, value_ref);
            };
        }

        assert_eq!(ValueRef::Null, Value::Null.as_value_ref());
        check_as_value_ref!(Boolean, true);
        check_as_value_ref!(UInt8, 123);
        check_as_value_ref!(UInt16, 123);
        check_as_value_ref!(UInt32, 123);
        check_as_value_ref!(UInt64, 123);
        check_as_value_ref!(Int8, -12);
        check_as_value_ref!(Int16, -12);
        check_as_value_ref!(Int32, -12);
        check_as_value_ref!(Int64, -12);
        check_as_value_ref!(Float32, OrderedF32::from(16.0));
        check_as_value_ref!(Float64, OrderedF64::from(16.0));
        check_as_value_ref!(Timestamp, Timestamp::new_millisecond(1));
        check_as_value_ref!(Time, Time::new_millisecond(1));
        check_as_value_ref!(Interval, Interval::from_month_day_nano(1, 2, 3));
        check_as_value_ref!(Duration, Duration::new_millisecond(1));

        assert_eq!(
            ValueRef::String("hello"),
            Value::String("hello".into()).as_value_ref()
        );
        assert_eq!(
            ValueRef::Binary(b"hello"),
            Value::Binary("hello".as_bytes().into()).as_value_ref()
        );

        check_as_value_ref!(Date, Date::new(103));
        check_as_value_ref!(DateTime, DateTime::new(1034));

        let list = ListValue {
            items: vec![],
            datatype: ConcreteDataType::int32_datatype(),
        };
        assert_eq!(
            ValueRef::List(ListValueRef::Ref { val: &list }),
            Value::List(list.clone()).as_value_ref()
        );

        let jsonb_value = jsonb::parse_value(r#"{"key": "value"}"#.as_bytes())
            .unwrap()
            .to_vec();
        assert_eq!(
            ValueRef::Binary(jsonb_value.clone().as_slice()),
            Value::Binary(jsonb_value.into()).as_value_ref()
        );
    }

    #[test]
    fn test_value_ref_as() {
        macro_rules! check_as_null {
            ($method: ident) => {
                assert_eq!(None, ValueRef::Null.$method().unwrap());
            };
        }

        check_as_null!(as_binary);
        check_as_null!(as_string);
        check_as_null!(as_boolean);
        check_as_null!(as_date);
        check_as_null!(as_datetime);
        check_as_null!(as_list);

        macro_rules! check_as_correct {
            ($data: expr, $Variant: ident, $method: ident) => {
                assert_eq!(Some($data), ValueRef::$Variant($data).$method().unwrap());
            };
        }

        check_as_correct!("hello", String, as_string);
        check_as_correct!("hello".as_bytes(), Binary, as_binary);
        check_as_correct!(true, Boolean, as_boolean);
        check_as_correct!(Date::new(123), Date, as_date);
        check_as_correct!(DateTime::new(12), DateTime, as_datetime);
        check_as_correct!(Time::new_second(12), Time, as_time);
        check_as_correct!(Duration::new_second(12), Duration, as_duration);
        let list = ListValue {
            items: vec![],
            datatype: ConcreteDataType::int32_datatype(),
        };
        check_as_correct!(ListValueRef::Ref { val: &list }, List, as_list);

        let wrong_value = ValueRef::Int32(12345);
        assert!(wrong_value.as_binary().is_err());
        assert!(wrong_value.as_string().is_err());
        assert!(wrong_value.as_boolean().is_err());
        assert!(wrong_value.as_date().is_err());
        assert!(wrong_value.as_datetime().is_err());
        assert!(wrong_value.as_list().is_err());
        assert!(wrong_value.as_time().is_err());
        assert!(wrong_value.as_timestamp().is_err());
    }

    #[test]
    fn test_display() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        assert_eq!(Value::Null.to_string(), "Null");
        assert_eq!(Value::UInt8(8).to_string(), "8");
        assert_eq!(Value::UInt16(16).to_string(), "16");
        assert_eq!(Value::UInt32(32).to_string(), "32");
        assert_eq!(Value::UInt64(64).to_string(), "64");
        assert_eq!(Value::Int8(-8).to_string(), "-8");
        assert_eq!(Value::Int16(-16).to_string(), "-16");
        assert_eq!(Value::Int32(-32).to_string(), "-32");
        assert_eq!(Value::Int64(-64).to_string(), "-64");
        assert_eq!(Value::Float32((-32.123).into()).to_string(), "-32.123");
        assert_eq!(Value::Float64((-64.123).into()).to_string(), "-64.123");
        assert_eq!(Value::Float64(OrderedF64::infinity()).to_string(), "inf");
        assert_eq!(Value::Float64(OrderedF64::nan()).to_string(), "NaN");
        assert_eq!(Value::String(StringBytes::from("123")).to_string(), "123");
        assert_eq!(
            Value::Binary(Bytes::from(vec![1, 2, 3])).to_string(),
            "010203"
        );
        assert_eq!(Value::Date(Date::new(0)).to_string(), "1970-01-01");
        assert_eq!(
            Value::DateTime(DateTime::new(0)).to_string(),
            "1970-01-01 08:00:00+0800"
        );
        assert_eq!(
            Value::Timestamp(Timestamp::new(1000, TimeUnit::Millisecond)).to_string(),
            "1970-01-01 08:00:01+0800"
        );
        assert_eq!(
            Value::Time(Time::new(1000, TimeUnit::Millisecond)).to_string(),
            "08:00:01+0800"
        );
        assert_eq!(
            Value::Duration(Duration::new_millisecond(1000)).to_string(),
            "1000ms"
        );
        assert_eq!(
            Value::List(ListValue::new(
                vec![Value::Int8(1), Value::Int8(2)],
                ConcreteDataType::int8_datatype(),
            ))
            .to_string(),
            "Int8[1, 2]"
        );
        assert_eq!(
            Value::List(ListValue::new(
                vec![],
                ConcreteDataType::timestamp_second_datatype(),
            ))
            .to_string(),
            "TimestampSecond[]"
        );
        assert_eq!(
            Value::List(ListValue::new(
                vec![],
                ConcreteDataType::timestamp_millisecond_datatype(),
            ))
            .to_string(),
            "TimestampMillisecond[]"
        );
        assert_eq!(
            Value::List(ListValue::new(
                vec![],
                ConcreteDataType::timestamp_microsecond_datatype(),
            ))
            .to_string(),
            "TimestampMicrosecond[]"
        );
        assert_eq!(
            Value::List(ListValue::new(
                vec![],
                ConcreteDataType::timestamp_nanosecond_datatype(),
            ))
            .to_string(),
            "TimestampNanosecond[]"
        );
    }

    #[test]
    fn test_not_null_value_to_scalar_value() {
        assert_eq!(
            ScalarValue::Boolean(Some(true)),
            Value::Boolean(true)
                .try_to_scalar_value(&ConcreteDataType::boolean_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Boolean(Some(false)),
            Value::Boolean(false)
                .try_to_scalar_value(&ConcreteDataType::boolean_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt8(Some(u8::MIN + 1)),
            Value::UInt8(u8::MIN + 1)
                .try_to_scalar_value(&ConcreteDataType::uint8_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt16(Some(u16::MIN + 2)),
            Value::UInt16(u16::MIN + 2)
                .try_to_scalar_value(&ConcreteDataType::uint16_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt32(Some(u32::MIN + 3)),
            Value::UInt32(u32::MIN + 3)
                .try_to_scalar_value(&ConcreteDataType::uint32_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt64(Some(u64::MIN + 4)),
            Value::UInt64(u64::MIN + 4)
                .try_to_scalar_value(&ConcreteDataType::uint64_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int8(Some(i8::MIN + 4)),
            Value::Int8(i8::MIN + 4)
                .try_to_scalar_value(&ConcreteDataType::int8_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int16(Some(i16::MIN + 5)),
            Value::Int16(i16::MIN + 5)
                .try_to_scalar_value(&ConcreteDataType::int16_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int32(Some(i32::MIN + 6)),
            Value::Int32(i32::MIN + 6)
                .try_to_scalar_value(&ConcreteDataType::int32_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(Some(i64::MIN + 7)),
            Value::Int64(i64::MIN + 7)
                .try_to_scalar_value(&ConcreteDataType::int64_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Float32(Some(8.0f32)),
            Value::Float32(OrderedFloat(8.0f32))
                .try_to_scalar_value(&ConcreteDataType::float32_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Float64(Some(9.0f64)),
            Value::Float64(OrderedFloat(9.0f64))
                .try_to_scalar_value(&ConcreteDataType::float64_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Utf8(Some("hello".to_string())),
            Value::String(StringBytes::from("hello"))
                .try_to_scalar_value(&ConcreteDataType::string_datatype(),)
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Binary(Some("world".as_bytes().to_vec())),
            Value::Binary(Bytes::from("world".as_bytes()))
                .try_to_scalar_value(&ConcreteDataType::binary_datatype())
                .unwrap()
        );

        let jsonb_value = jsonb::parse_value(r#"{"key": "value"}"#.as_bytes())
            .unwrap()
            .to_vec();
        assert_eq!(
            ScalarValue::Binary(Some(jsonb_value.clone())),
            Value::Binary(jsonb_value.into())
                .try_to_scalar_value(&ConcreteDataType::json_datatype())
                .unwrap()
        );
    }

    #[test]
    fn test_null_value_to_scalar_value() {
        assert_eq!(
            ScalarValue::Boolean(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::boolean_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt8(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::uint8_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt16(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::uint16_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt32(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::uint32_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::UInt64(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::uint64_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int8(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::int8_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int16(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::int16_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int32(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::int32_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::int64_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Float32(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::float32_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Float64(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::float64_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Utf8(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::string_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Binary(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::binary_datatype())
                .unwrap()
        );

        assert_eq!(
            ScalarValue::Time32Second(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::time_second_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Time32Millisecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::time_millisecond_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Time64Microsecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::time_microsecond_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Time64Nanosecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::time_nanosecond_datatype())
                .unwrap()
        );

        assert_eq!(
            ScalarValue::DurationSecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::duration_second_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::DurationMillisecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::duration_millisecond_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::DurationMicrosecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::duration_microsecond_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::DurationNanosecond(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::duration_nanosecond_datatype())
                .unwrap()
        );
        assert_eq!(
            ScalarValue::Binary(None),
            Value::Null
                .try_to_scalar_value(&ConcreteDataType::json_datatype())
                .unwrap()
        );
    }

    #[test]
    fn test_list_value_to_scalar_value() {
        let items = vec![Value::Int32(-1), Value::Null];
        let list = Value::List(ListValue::new(items, ConcreteDataType::int32_datatype()));
        let df_list = list
            .try_to_scalar_value(&ConcreteDataType::list_datatype(
                ConcreteDataType::int32_datatype(),
            ))
            .unwrap();
        assert!(matches!(df_list, ScalarValue::List(_)));
        match df_list {
            ScalarValue::List(vs) => {
                assert_eq!(
                    ArrowDataType::List(Arc::new(Field::new_list_field(
                        ArrowDataType::Int32,
                        true
                    ))),
                    *vs.data_type()
                );

                let vs = ScalarValue::convert_array_to_scalar_vec(vs.as_ref())
                    .unwrap()
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>();
                assert_eq!(
                    vs,
                    vec![ScalarValue::Int32(Some(-1)), ScalarValue::Int32(None)]
                );
            }
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_timestamp_to_scalar_value() {
        assert_eq!(
            ScalarValue::TimestampSecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Second, Some(1))
        );
        assert_eq!(
            ScalarValue::TimestampMillisecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Millisecond, Some(1))
        );
        assert_eq!(
            ScalarValue::TimestampMicrosecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Microsecond, Some(1))
        );
        assert_eq!(
            ScalarValue::TimestampNanosecond(Some(1), None),
            timestamp_to_scalar_value(TimeUnit::Nanosecond, Some(1))
        );
    }

    #[test]
    fn test_time_to_scalar_value() {
        assert_eq!(
            ScalarValue::Time32Second(Some(1)),
            time_to_scalar_value(TimeUnit::Second, Some(1)).unwrap()
        );
        assert_eq!(
            ScalarValue::Time32Millisecond(Some(1)),
            time_to_scalar_value(TimeUnit::Millisecond, Some(1)).unwrap()
        );
        assert_eq!(
            ScalarValue::Time64Microsecond(Some(1)),
            time_to_scalar_value(TimeUnit::Microsecond, Some(1)).unwrap()
        );
        assert_eq!(
            ScalarValue::Time64Nanosecond(Some(1)),
            time_to_scalar_value(TimeUnit::Nanosecond, Some(1)).unwrap()
        );
    }

    #[test]
    fn test_duration_to_scalar_value() {
        assert_eq!(
            ScalarValue::DurationSecond(Some(1)),
            duration_to_scalar_value(TimeUnit::Second, Some(1))
        );
        assert_eq!(
            ScalarValue::DurationMillisecond(Some(1)),
            duration_to_scalar_value(TimeUnit::Millisecond, Some(1))
        );
        assert_eq!(
            ScalarValue::DurationMicrosecond(Some(1)),
            duration_to_scalar_value(TimeUnit::Microsecond, Some(1))
        );
        assert_eq!(
            ScalarValue::DurationNanosecond(Some(1)),
            duration_to_scalar_value(TimeUnit::Nanosecond, Some(1))
        );
    }

    fn check_value_ref_size_eq(value_ref: &ValueRef, size: usize) {
        assert_eq!(value_ref.data_size(), size);
    }

    #[test]
    fn test_value_ref_estimated_size() {
        check_value_ref_size_eq(&ValueRef::Null, 8);
        check_value_ref_size_eq(&ValueRef::Boolean(true), 1);
        check_value_ref_size_eq(&ValueRef::UInt8(1), 1);
        check_value_ref_size_eq(&ValueRef::UInt16(1), 2);
        check_value_ref_size_eq(&ValueRef::UInt32(1), 4);
        check_value_ref_size_eq(&ValueRef::UInt64(1), 8);
        check_value_ref_size_eq(&ValueRef::Int8(1), 1);
        check_value_ref_size_eq(&ValueRef::Int16(1), 2);
        check_value_ref_size_eq(&ValueRef::Int32(1), 4);
        check_value_ref_size_eq(&ValueRef::Int64(1), 8);
        check_value_ref_size_eq(&ValueRef::Float32(1.0.into()), 4);
        check_value_ref_size_eq(&ValueRef::Float64(1.0.into()), 8);
        check_value_ref_size_eq(&ValueRef::String("greptimedb"), 10);
        check_value_ref_size_eq(&ValueRef::Binary(b"greptimedb"), 10);
        check_value_ref_size_eq(&ValueRef::Date(Date::new(1)), 4);
        check_value_ref_size_eq(&ValueRef::DateTime(DateTime::new(1)), 8);
        check_value_ref_size_eq(&ValueRef::Timestamp(Timestamp::new_millisecond(1)), 16);
        check_value_ref_size_eq(&ValueRef::Time(Time::new_millisecond(1)), 16);
        check_value_ref_size_eq(
            &ValueRef::Interval(Interval::from_month_day_nano(1, 2, 3)),
            24,
        );
        check_value_ref_size_eq(&ValueRef::Duration(Duration::new_millisecond(1)), 16);
        check_value_ref_size_eq(
            &ValueRef::List(ListValueRef::Ref {
                val: &ListValue {
                    items: vec![
                        Value::String("hello world".into()),
                        Value::String("greptimedb".into()),
                    ],
                    datatype: ConcreteDataType::string_datatype(),
                },
            }),
            22,
        );

        let data = vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
            Some(vec![Some(4), None, Some(6)]),
        ];
        let mut builder =
            ListVectorBuilder::with_type_capacity(ConcreteDataType::int32_datatype(), 8);
        for vec_opt in &data {
            if let Some(vec) = vec_opt {
                let values = vec.iter().map(|v| Value::from(*v)).collect();
                let list_value = ListValue::new(values, ConcreteDataType::int32_datatype());

                builder.push(Some(ListValueRef::Ref { val: &list_value }));
            } else {
                builder.push(None);
            }
        }
        let vector = builder.finish();

        check_value_ref_size_eq(
            &ValueRef::List(ListValueRef::Indexed {
                vector: &vector,
                idx: 0,
            }),
            85,
        );
        check_value_ref_size_eq(
            &ValueRef::List(ListValueRef::Indexed {
                vector: &vector,
                idx: 1,
            }),
            85,
        );
        check_value_ref_size_eq(
            &ValueRef::List(ListValueRef::Indexed {
                vector: &vector,
                idx: 2,
            }),
            85,
        );
        check_value_ref_size_eq(&ValueRef::Decimal128(Decimal128::new(1234, 3, 1)), 32)
    }

    #[test]
    fn test_incorrect_default_value_issue_3479() {
        let value = OrderedF64::from(0.047318541668048164);
        let serialized = serde_json::to_string(&value).unwrap();
        let deserialized: OrderedF64 = serde_json::from_str(&serialized).unwrap();
        assert_eq!(value, deserialized);
    }
}
