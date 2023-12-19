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

use std::sync::Arc;

use common_base::BitVec;
use common_decimal::decimal128::{DECIMAL128_DEFAULT_SCALE, DECIMAL128_MAX_PRECISION};
use common_decimal::Decimal128;
use common_time::interval::IntervalUnit;
use common_time::time::Time;
use common_time::timestamp::TimeUnit;
use common_time::{Date, DateTime, Duration, Interval, Timestamp};
use datatypes::prelude::{ConcreteDataType, ValueRef};
use datatypes::scalars::ScalarVector;
use datatypes::types::{
    DurationType, Int16Type, Int8Type, IntervalType, TimeType, TimestampType, UInt16Type, UInt8Type,
};
use datatypes::value::{OrderedF32, OrderedF64, Value};
use datatypes::vectors::{
    BinaryVector, BooleanVector, DateTimeVector, DateVector, Decimal128Vector,
    DurationMicrosecondVector, DurationMillisecondVector, DurationNanosecondVector,
    DurationSecondVector, Float32Vector, Float64Vector, Int32Vector, Int64Vector,
    IntervalDayTimeVector, IntervalMonthDayNanoVector, IntervalYearMonthVector, PrimitiveVector,
    StringVector, TimeMicrosecondVector, TimeMillisecondVector, TimeNanosecondVector,
    TimeSecondVector, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, UInt32Vector, UInt64Vector, VectorRef,
};
use greptime_proto::v1;
use greptime_proto::v1::column_data_type_extension::TypeExt;
use greptime_proto::v1::ddl_request::Expr;
use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::query_request::Query;
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{
    ColumnDataTypeExtension, DdlRequest, DecimalTypeExtension, QueryRequest, Row, SemanticType,
};
use paste::paste;
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::v1::column::Values;
use crate::v1::{Column, ColumnDataType, Value as GrpcValue};

/// ColumnDataTypeWrapper is a wrapper of ColumnDataType and ColumnDataTypeExtension.
/// It could be used to convert with ConcreteDataType.
#[derive(Debug, PartialEq)]
pub struct ColumnDataTypeWrapper {
    datatype: ColumnDataType,
    datatype_ext: Option<ColumnDataTypeExtension>,
}

impl ColumnDataTypeWrapper {
    /// Try to create a ColumnDataTypeWrapper from i32(ColumnDataType) and ColumnDataTypeExtension.
    pub fn try_new(datatype: i32, datatype_ext: Option<ColumnDataTypeExtension>) -> Result<Self> {
        let datatype = ColumnDataType::try_from(datatype)
            .context(error::UnknownColumnDataTypeSnafu { datatype })?;
        Ok(Self {
            datatype,
            datatype_ext,
        })
    }

    /// Create a ColumnDataTypeWrapper from ColumnDataType and ColumnDataTypeExtension.
    pub fn new(datatype: ColumnDataType, datatype_ext: Option<ColumnDataTypeExtension>) -> Self {
        Self {
            datatype,
            datatype_ext,
        }
    }

    /// Get the ColumnDataType.
    pub fn datatype(&self) -> ColumnDataType {
        self.datatype
    }

    /// Get a tuple of ColumnDataType and ColumnDataTypeExtension.
    pub fn to_parts(&self) -> (ColumnDataType, Option<ColumnDataTypeExtension>) {
        (self.datatype, self.datatype_ext.clone())
    }
}

impl From<ColumnDataTypeWrapper> for ConcreteDataType {
    fn from(datatype_wrapper: ColumnDataTypeWrapper) -> Self {
        match datatype_wrapper.datatype {
            ColumnDataType::Boolean => ConcreteDataType::boolean_datatype(),
            ColumnDataType::Int8 => ConcreteDataType::int8_datatype(),
            ColumnDataType::Int16 => ConcreteDataType::int16_datatype(),
            ColumnDataType::Int32 => ConcreteDataType::int32_datatype(),
            ColumnDataType::Int64 => ConcreteDataType::int64_datatype(),
            ColumnDataType::Uint8 => ConcreteDataType::uint8_datatype(),
            ColumnDataType::Uint16 => ConcreteDataType::uint16_datatype(),
            ColumnDataType::Uint32 => ConcreteDataType::uint32_datatype(),
            ColumnDataType::Uint64 => ConcreteDataType::uint64_datatype(),
            ColumnDataType::Float32 => ConcreteDataType::float32_datatype(),
            ColumnDataType::Float64 => ConcreteDataType::float64_datatype(),
            ColumnDataType::Binary => ConcreteDataType::binary_datatype(),
            ColumnDataType::String => ConcreteDataType::string_datatype(),
            ColumnDataType::Date => ConcreteDataType::date_datatype(),
            ColumnDataType::Datetime => ConcreteDataType::datetime_datatype(),
            ColumnDataType::TimestampSecond => ConcreteDataType::timestamp_second_datatype(),
            ColumnDataType::TimestampMillisecond => {
                ConcreteDataType::timestamp_millisecond_datatype()
            }
            ColumnDataType::TimestampMicrosecond => {
                ConcreteDataType::timestamp_microsecond_datatype()
            }
            ColumnDataType::TimestampNanosecond => {
                ConcreteDataType::timestamp_nanosecond_datatype()
            }
            ColumnDataType::TimeSecond => ConcreteDataType::time_second_datatype(),
            ColumnDataType::TimeMillisecond => ConcreteDataType::time_millisecond_datatype(),
            ColumnDataType::TimeMicrosecond => ConcreteDataType::time_microsecond_datatype(),
            ColumnDataType::TimeNanosecond => ConcreteDataType::time_nanosecond_datatype(),
            ColumnDataType::IntervalYearMonth => ConcreteDataType::interval_year_month_datatype(),
            ColumnDataType::IntervalDayTime => ConcreteDataType::interval_day_time_datatype(),
            ColumnDataType::IntervalMonthDayNano => {
                ConcreteDataType::interval_month_day_nano_datatype()
            }
            ColumnDataType::DurationSecond => ConcreteDataType::duration_second_datatype(),
            ColumnDataType::DurationMillisecond => {
                ConcreteDataType::duration_millisecond_datatype()
            }
            ColumnDataType::DurationMicrosecond => {
                ConcreteDataType::duration_microsecond_datatype()
            }
            ColumnDataType::DurationNanosecond => ConcreteDataType::duration_nanosecond_datatype(),
            ColumnDataType::Decimal128 => {
                if let Some(TypeExt::DecimalType(d)) = datatype_wrapper
                    .datatype_ext
                    .as_ref()
                    .and_then(|datatype_ext| datatype_ext.type_ext.as_ref())
                {
                    ConcreteDataType::decimal128_datatype(d.precision as u8, d.scale as i8)
                } else {
                    ConcreteDataType::decimal128_default_datatype()
                }
            }
        }
    }
}

/// This macro is used to generate datatype functions
/// with lower style for ColumnDataTypeWrapper.
///
///
/// For example: we can use `ColumnDataTypeWrapper::int8_datatype()`,
/// to get a ColumnDataTypeWrapper with datatype `ColumnDataType::Int8`.
macro_rules! impl_column_type_functions {
    ($($Type: ident), +) => {
        paste! {
            impl ColumnDataTypeWrapper {
                $(
                    pub fn [<$Type:lower _datatype>]() -> ColumnDataTypeWrapper {
                        ColumnDataTypeWrapper {
                            datatype: ColumnDataType::$Type,
                            datatype_ext: None,
                        }
                    }
                )+
            }
        }
    }
}

/// This macro is used to generate datatype functions
/// with snake style for ColumnDataTypeWrapper.
///
///
/// For example: we can use `ColumnDataTypeWrapper::duration_second_datatype()`,
/// to get a ColumnDataTypeWrapper with datatype `ColumnDataType::DurationSecond`.
macro_rules! impl_column_type_functions_with_snake {
    ($($TypeName: ident), +) => {
        paste!{
            impl ColumnDataTypeWrapper {
                $(
                    pub fn [<$TypeName:snake _datatype>]() -> ColumnDataTypeWrapper {
                        ColumnDataTypeWrapper {
                            datatype: ColumnDataType::$TypeName,
                            datatype_ext: None,
                        }
                    }
                )+
            }
        }
    };
}

impl_column_type_functions!(
    Boolean, Uint8, Uint16, Uint32, Uint64, Int8, Int16, Int32, Int64, Float32, Float64, Binary,
    Date, Datetime, String
);

impl_column_type_functions_with_snake!(
    TimestampSecond,
    TimestampMillisecond,
    TimestampMicrosecond,
    TimestampNanosecond,
    TimeSecond,
    TimeMillisecond,
    TimeMicrosecond,
    TimeNanosecond,
    IntervalYearMonth,
    IntervalDayTime,
    IntervalMonthDayNano,
    DurationSecond,
    DurationMillisecond,
    DurationMicrosecond,
    DurationNanosecond
);

impl ColumnDataTypeWrapper {
    pub fn decimal128_datatype(precision: i32, scale: i32) -> Self {
        ColumnDataTypeWrapper {
            datatype: ColumnDataType::Decimal128,
            datatype_ext: Some(ColumnDataTypeExtension {
                type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                    precision,
                    scale,
                })),
            }),
        }
    }
}

impl TryFrom<ConcreteDataType> for ColumnDataTypeWrapper {
    type Error = error::Error;

    fn try_from(datatype: ConcreteDataType) -> Result<Self> {
        let column_datatype = match datatype {
            ConcreteDataType::Boolean(_) => ColumnDataType::Boolean,
            ConcreteDataType::Int8(_) => ColumnDataType::Int8,
            ConcreteDataType::Int16(_) => ColumnDataType::Int16,
            ConcreteDataType::Int32(_) => ColumnDataType::Int32,
            ConcreteDataType::Int64(_) => ColumnDataType::Int64,
            ConcreteDataType::UInt8(_) => ColumnDataType::Uint8,
            ConcreteDataType::UInt16(_) => ColumnDataType::Uint16,
            ConcreteDataType::UInt32(_) => ColumnDataType::Uint32,
            ConcreteDataType::UInt64(_) => ColumnDataType::Uint64,
            ConcreteDataType::Float32(_) => ColumnDataType::Float32,
            ConcreteDataType::Float64(_) => ColumnDataType::Float64,
            ConcreteDataType::Binary(_) => ColumnDataType::Binary,
            ConcreteDataType::String(_) => ColumnDataType::String,
            ConcreteDataType::Date(_) => ColumnDataType::Date,
            ConcreteDataType::DateTime(_) => ColumnDataType::Datetime,
            ConcreteDataType::Timestamp(t) => match t {
                TimestampType::Second(_) => ColumnDataType::TimestampSecond,
                TimestampType::Millisecond(_) => ColumnDataType::TimestampMillisecond,
                TimestampType::Microsecond(_) => ColumnDataType::TimestampMicrosecond,
                TimestampType::Nanosecond(_) => ColumnDataType::TimestampNanosecond,
            },
            ConcreteDataType::Time(t) => match t {
                TimeType::Second(_) => ColumnDataType::TimeSecond,
                TimeType::Millisecond(_) => ColumnDataType::TimeMillisecond,
                TimeType::Microsecond(_) => ColumnDataType::TimeMicrosecond,
                TimeType::Nanosecond(_) => ColumnDataType::TimeNanosecond,
            },
            ConcreteDataType::Interval(i) => match i {
                IntervalType::YearMonth(_) => ColumnDataType::IntervalYearMonth,
                IntervalType::DayTime(_) => ColumnDataType::IntervalDayTime,
                IntervalType::MonthDayNano(_) => ColumnDataType::IntervalMonthDayNano,
            },
            ConcreteDataType::Duration(d) => match d {
                DurationType::Second(_) => ColumnDataType::DurationSecond,
                DurationType::Millisecond(_) => ColumnDataType::DurationMillisecond,
                DurationType::Microsecond(_) => ColumnDataType::DurationMicrosecond,
                DurationType::Nanosecond(_) => ColumnDataType::DurationNanosecond,
            },
            ConcreteDataType::Decimal128(_) => ColumnDataType::Decimal128,
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => {
                return error::IntoColumnDataTypeSnafu { from: datatype }.fail()
            }
        };
        let datatype_extension = match column_datatype {
            ColumnDataType::Decimal128 => {
                datatype
                    .as_decimal128()
                    .map(|decimal_type| ColumnDataTypeExtension {
                        type_ext: Some(TypeExt::DecimalType(DecimalTypeExtension {
                            precision: decimal_type.precision() as i32,
                            scale: decimal_type.scale() as i32,
                        })),
                    })
            }
            _ => None,
        };
        Ok(Self {
            datatype: column_datatype,
            datatype_ext: datatype_extension,
        })
    }
}

pub fn values_with_capacity(datatype: ColumnDataType, capacity: usize) -> Values {
    match datatype {
        ColumnDataType::Boolean => Values {
            bool_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int8 => Values {
            i8_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int16 => Values {
            i16_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int32 => Values {
            i32_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Int64 => Values {
            i64_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint8 => Values {
            u8_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint16 => Values {
            u16_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint32 => Values {
            u32_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Uint64 => Values {
            u64_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Float32 => Values {
            f32_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Float64 => Values {
            f64_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Binary => Values {
            binary_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::String => Values {
            string_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Date => Values {
            date_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Datetime => Values {
            datetime_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampSecond => Values {
            timestamp_second_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampMillisecond => Values {
            timestamp_millisecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampMicrosecond => Values {
            timestamp_microsecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampNanosecond => Values {
            timestamp_nanosecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeSecond => Values {
            time_second_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeMillisecond => Values {
            time_millisecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeMicrosecond => Values {
            time_microsecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimeNanosecond => Values {
            time_nanosecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::IntervalDayTime => Values {
            interval_day_time_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::IntervalYearMonth => Values {
            interval_year_month_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::IntervalMonthDayNano => Values {
            interval_month_day_nano_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::DurationSecond => Values {
            duration_second_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::DurationMillisecond => Values {
            duration_millisecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::DurationMicrosecond => Values {
            duration_microsecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::DurationNanosecond => Values {
            duration_nanosecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::Decimal128 => Values {
            decimal128_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
    }
}

// The type of vals must be same.
pub fn push_vals(column: &mut Column, origin_count: usize, vector: VectorRef) {
    let values = column.values.get_or_insert_with(Values::default);
    let mut null_mask = BitVec::from_slice(&column.null_mask);
    let len = vector.len();
    null_mask.reserve_exact(origin_count + len);
    null_mask.extend(BitVec::repeat(false, len));

    (0..len).for_each(|idx| match vector.get(idx) {
        Value::Null => null_mask.set(idx + origin_count, true),
        Value::Boolean(val) => values.bool_values.push(val),
        Value::UInt8(val) => values.u8_values.push(val.into()),
        Value::UInt16(val) => values.u16_values.push(val.into()),
        Value::UInt32(val) => values.u32_values.push(val),
        Value::UInt64(val) => values.u64_values.push(val),
        Value::Int8(val) => values.i8_values.push(val.into()),
        Value::Int16(val) => values.i16_values.push(val.into()),
        Value::Int32(val) => values.i32_values.push(val),
        Value::Int64(val) => values.i64_values.push(val),
        Value::Float32(val) => values.f32_values.push(*val),
        Value::Float64(val) => values.f64_values.push(*val),
        Value::String(val) => values.string_values.push(val.as_utf8().to_string()),
        Value::Binary(val) => values.binary_values.push(val.to_vec()),
        Value::Date(val) => values.date_values.push(val.val()),
        Value::DateTime(val) => values.datetime_values.push(val.val()),
        Value::Timestamp(val) => match val.unit() {
            TimeUnit::Second => values.timestamp_second_values.push(val.value()),
            TimeUnit::Millisecond => values.timestamp_millisecond_values.push(val.value()),
            TimeUnit::Microsecond => values.timestamp_microsecond_values.push(val.value()),
            TimeUnit::Nanosecond => values.timestamp_nanosecond_values.push(val.value()),
        },
        Value::Time(val) => match val.unit() {
            TimeUnit::Second => values.time_second_values.push(val.value()),
            TimeUnit::Millisecond => values.time_millisecond_values.push(val.value()),
            TimeUnit::Microsecond => values.time_microsecond_values.push(val.value()),
            TimeUnit::Nanosecond => values.time_nanosecond_values.push(val.value()),
        },
        Value::Interval(val) => match val.unit() {
            IntervalUnit::YearMonth => values.interval_year_month_values.push(val.to_i32()),
            IntervalUnit::DayTime => values.interval_day_time_values.push(val.to_i64()),
            IntervalUnit::MonthDayNano => values
                .interval_month_day_nano_values
                .push(convert_i128_to_interval(val.to_i128())),
        },
        Value::Duration(val) => match val.unit() {
            TimeUnit::Second => values.duration_second_values.push(val.value()),
            TimeUnit::Millisecond => values.duration_millisecond_values.push(val.value()),
            TimeUnit::Microsecond => values.duration_microsecond_values.push(val.value()),
            TimeUnit::Nanosecond => values.duration_nanosecond_values.push(val.value()),
        },
        Value::Decimal128(val) => values.decimal128_values.push(convert_to_pb_decimal128(val)),
        Value::List(_) => unreachable!(),
    });
    column.null_mask = null_mask.into_vec();
}

/// Returns the type name of the [Request].
pub fn request_type(request: &Request) -> &'static str {
    match request {
        Request::Inserts(_) => "inserts",
        Request::Query(query_req) => query_request_type(query_req),
        Request::Ddl(ddl_req) => ddl_request_type(ddl_req),
        Request::Deletes(_) => "deletes",
        Request::RowInserts(_) => "row_inserts",
        Request::RowDeletes(_) => "row_deletes",
    }
}

/// Returns the type name of the [QueryRequest].
fn query_request_type(request: &QueryRequest) -> &'static str {
    match request.query {
        Some(Query::Sql(_)) => "query.sql",
        Some(Query::LogicalPlan(_)) => "query.logical_plan",
        Some(Query::PromRangeQuery(_)) => "query.prom_range",
        None => "query.empty",
    }
}

/// Returns the type name of the [DdlRequest].
fn ddl_request_type(request: &DdlRequest) -> &'static str {
    match request.expr {
        Some(Expr::CreateDatabase(_)) => "ddl.create_database",
        Some(Expr::CreateTable(_)) => "ddl.create_table",
        Some(Expr::Alter(_)) => "ddl.alter",
        Some(Expr::DropTable(_)) => "ddl.drop_table",
        Some(Expr::TruncateTable(_)) => "ddl.truncate_table",
        None => "ddl.empty",
    }
}

/// Converts an i128 value to google protobuf type [IntervalMonthDayNano].
pub fn convert_i128_to_interval(v: i128) -> v1::IntervalMonthDayNano {
    let interval = Interval::from_i128(v);
    let (months, days, nanoseconds) = interval.to_month_day_nano();
    v1::IntervalMonthDayNano {
        months,
        days,
        nanoseconds,
    }
}

/// Convert common decimal128 to grpc decimal128 without precision and scale.
pub fn convert_to_pb_decimal128(v: Decimal128) -> v1::Decimal128 {
    let (hi, lo) = v.split_value();
    v1::Decimal128 { hi, lo }
}

pub fn pb_value_to_value_ref<'a>(
    value: &'a v1::Value,
    datatype_ext: &'a Option<ColumnDataTypeExtension>,
) -> ValueRef<'a> {
    let Some(value) = &value.value_data else {
        return ValueRef::Null;
    };

    match value {
        ValueData::I8Value(v) => ValueRef::Int8(*v as i8),
        ValueData::I16Value(v) => ValueRef::Int16(*v as i16),
        ValueData::I32Value(v) => ValueRef::Int32(*v),
        ValueData::I64Value(v) => ValueRef::Int64(*v),
        ValueData::U8Value(v) => ValueRef::UInt8(*v as u8),
        ValueData::U16Value(v) => ValueRef::UInt16(*v as u16),
        ValueData::U32Value(v) => ValueRef::UInt32(*v),
        ValueData::U64Value(v) => ValueRef::UInt64(*v),
        ValueData::F32Value(f) => ValueRef::Float32(OrderedF32::from(*f)),
        ValueData::F64Value(f) => ValueRef::Float64(OrderedF64::from(*f)),
        ValueData::BoolValue(b) => ValueRef::Boolean(*b),
        ValueData::BinaryValue(bytes) => ValueRef::Binary(bytes.as_slice()),
        ValueData::StringValue(string) => ValueRef::String(string.as_str()),
        ValueData::DateValue(d) => ValueRef::Date(Date::from(*d)),
        ValueData::DatetimeValue(d) => ValueRef::DateTime(DateTime::new(*d)),
        ValueData::TimestampSecondValue(t) => ValueRef::Timestamp(Timestamp::new_second(*t)),
        ValueData::TimestampMillisecondValue(t) => {
            ValueRef::Timestamp(Timestamp::new_millisecond(*t))
        }
        ValueData::TimestampMicrosecondValue(t) => {
            ValueRef::Timestamp(Timestamp::new_microsecond(*t))
        }
        ValueData::TimestampNanosecondValue(t) => {
            ValueRef::Timestamp(Timestamp::new_nanosecond(*t))
        }
        ValueData::TimeSecondValue(t) => ValueRef::Time(Time::new_second(*t)),
        ValueData::TimeMillisecondValue(t) => ValueRef::Time(Time::new_millisecond(*t)),
        ValueData::TimeMicrosecondValue(t) => ValueRef::Time(Time::new_microsecond(*t)),
        ValueData::TimeNanosecondValue(t) => ValueRef::Time(Time::new_nanosecond(*t)),
        ValueData::IntervalYearMonthValue(v) => ValueRef::Interval(Interval::from_i32(*v)),
        ValueData::IntervalDayTimeValue(v) => ValueRef::Interval(Interval::from_i64(*v)),
        ValueData::IntervalMonthDayNanoValue(v) => {
            let interval = Interval::from_month_day_nano(v.months, v.days, v.nanoseconds);
            ValueRef::Interval(interval)
        }
        ValueData::DurationSecondValue(v) => ValueRef::Duration(Duration::new_second(*v)),
        ValueData::DurationMillisecondValue(v) => ValueRef::Duration(Duration::new_millisecond(*v)),
        ValueData::DurationMicrosecondValue(v) => ValueRef::Duration(Duration::new_microsecond(*v)),
        ValueData::DurationNanosecondValue(v) => ValueRef::Duration(Duration::new_nanosecond(*v)),
        ValueData::Decimal128Value(v) => {
            // get precision and scale from datatype_extension
            if let Some(TypeExt::DecimalType(d)) = datatype_ext
                .as_ref()
                .and_then(|column_ext| column_ext.type_ext.as_ref())
            {
                ValueRef::Decimal128(Decimal128::from_value_precision_scale(
                    v.hi,
                    v.lo,
                    d.precision as u8,
                    d.scale as i8,
                ))
            } else {
                // If the precision and scale are not set, use the default value.
                ValueRef::Decimal128(Decimal128::from_value_precision_scale(
                    v.hi,
                    v.lo,
                    DECIMAL128_MAX_PRECISION,
                    DECIMAL128_DEFAULT_SCALE,
                ))
            }
        }
    }
}

pub fn pb_values_to_vector_ref(data_type: &ConcreteDataType, values: Values) -> VectorRef {
    match data_type {
        ConcreteDataType::Boolean(_) => Arc::new(BooleanVector::from(values.bool_values)),
        ConcreteDataType::Int8(_) => Arc::new(PrimitiveVector::<Int8Type>::from_iter_values(
            values.i8_values.into_iter().map(|x| x as i8),
        )),
        ConcreteDataType::Int16(_) => Arc::new(PrimitiveVector::<Int16Type>::from_iter_values(
            values.i16_values.into_iter().map(|x| x as i16),
        )),
        ConcreteDataType::Int32(_) => Arc::new(Int32Vector::from_vec(values.i32_values)),
        ConcreteDataType::Int64(_) => Arc::new(Int64Vector::from_vec(values.i64_values)),
        ConcreteDataType::UInt8(_) => Arc::new(PrimitiveVector::<UInt8Type>::from_iter_values(
            values.u8_values.into_iter().map(|x| x as u8),
        )),
        ConcreteDataType::UInt16(_) => Arc::new(PrimitiveVector::<UInt16Type>::from_iter_values(
            values.u16_values.into_iter().map(|x| x as u16),
        )),
        ConcreteDataType::UInt32(_) => Arc::new(UInt32Vector::from_vec(values.u32_values)),
        ConcreteDataType::UInt64(_) => Arc::new(UInt64Vector::from_vec(values.u64_values)),
        ConcreteDataType::Float32(_) => Arc::new(Float32Vector::from_vec(values.f32_values)),
        ConcreteDataType::Float64(_) => Arc::new(Float64Vector::from_vec(values.f64_values)),
        ConcreteDataType::Binary(_) => Arc::new(BinaryVector::from(values.binary_values)),
        ConcreteDataType::String(_) => Arc::new(StringVector::from_vec(values.string_values)),
        ConcreteDataType::Date(_) => Arc::new(DateVector::from_vec(values.date_values)),
        ConcreteDataType::DateTime(_) => Arc::new(DateTimeVector::from_vec(values.datetime_values)),
        ConcreteDataType::Timestamp(unit) => match unit {
            TimestampType::Second(_) => Arc::new(TimestampSecondVector::from_vec(
                values.timestamp_second_values,
            )),
            TimestampType::Millisecond(_) => Arc::new(TimestampMillisecondVector::from_vec(
                values.timestamp_millisecond_values,
            )),
            TimestampType::Microsecond(_) => Arc::new(TimestampMicrosecondVector::from_vec(
                values.timestamp_microsecond_values,
            )),
            TimestampType::Nanosecond(_) => Arc::new(TimestampNanosecondVector::from_vec(
                values.timestamp_nanosecond_values,
            )),
        },
        ConcreteDataType::Time(unit) => match unit {
            TimeType::Second(_) => Arc::new(TimeSecondVector::from_iter_values(
                values.time_second_values.iter().map(|x| *x as i32),
            )),
            TimeType::Millisecond(_) => Arc::new(TimeMillisecondVector::from_iter_values(
                values.time_millisecond_values.iter().map(|x| *x as i32),
            )),
            TimeType::Microsecond(_) => Arc::new(TimeMicrosecondVector::from_vec(
                values.time_microsecond_values,
            )),
            TimeType::Nanosecond(_) => Arc::new(TimeNanosecondVector::from_vec(
                values.time_nanosecond_values,
            )),
        },

        ConcreteDataType::Interval(unit) => match unit {
            IntervalType::YearMonth(_) => Arc::new(IntervalYearMonthVector::from_vec(
                values.interval_year_month_values,
            )),
            IntervalType::DayTime(_) => Arc::new(IntervalDayTimeVector::from_vec(
                values.interval_day_time_values,
            )),
            IntervalType::MonthDayNano(_) => {
                Arc::new(IntervalMonthDayNanoVector::from_iter_values(
                    values.interval_month_day_nano_values.iter().map(|x| {
                        Interval::from_month_day_nano(x.months, x.days, x.nanoseconds).to_i128()
                    }),
                ))
            }
        },
        ConcreteDataType::Duration(unit) => match unit {
            DurationType::Second(_) => Arc::new(DurationSecondVector::from_vec(
                values.duration_second_values,
            )),
            DurationType::Millisecond(_) => Arc::new(DurationMillisecondVector::from_vec(
                values.duration_millisecond_values,
            )),
            DurationType::Microsecond(_) => Arc::new(DurationMicrosecondVector::from_vec(
                values.duration_microsecond_values,
            )),
            DurationType::Nanosecond(_) => Arc::new(DurationNanosecondVector::from_vec(
                values.duration_nanosecond_values,
            )),
        },
        ConcreteDataType::Decimal128(d) => Arc::new(Decimal128Vector::from_values(
            values.decimal128_values.iter().map(|x| {
                Decimal128::from_value_precision_scale(x.hi, x.lo, d.precision(), d.scale()).into()
            }),
        )),
        ConcreteDataType::Null(_) | ConcreteDataType::List(_) | ConcreteDataType::Dictionary(_) => {
            unreachable!()
        }
    }
}

pub fn pb_values_to_values(data_type: &ConcreteDataType, values: Values) -> Vec<Value> {
    // TODO(fys): use macros to optimize code
    match data_type {
        ConcreteDataType::Int64(_) => values
            .i64_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Float64(_) => values
            .f64_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::String(_) => values
            .string_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Boolean(_) => values
            .bool_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Int8(_) => values
            .i8_values
            .into_iter()
            // Safety: Since i32 only stores i8 data here, so i32 as i8 is safe.
            .map(|val| (val as i8).into())
            .collect(),
        ConcreteDataType::Int16(_) => values
            .i16_values
            .into_iter()
            // Safety: Since i32 only stores i16 data here, so i32 as i16 is safe.
            .map(|val| (val as i16).into())
            .collect(),
        ConcreteDataType::Int32(_) => values
            .i32_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::UInt8(_) => values
            .u8_values
            .into_iter()
            // Safety: Since i32 only stores u8 data here, so i32 as u8 is safe.
            .map(|val| (val as u8).into())
            .collect(),
        ConcreteDataType::UInt16(_) => values
            .u16_values
            .into_iter()
            // Safety: Since i32 only stores u16 data here, so i32 as u16 is safe.
            .map(|val| (val as u16).into())
            .collect(),
        ConcreteDataType::UInt32(_) => values
            .u32_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::UInt64(_) => values
            .u64_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Float32(_) => values
            .f32_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::Binary(_) => values
            .binary_values
            .into_iter()
            .map(|val| val.into())
            .collect(),
        ConcreteDataType::DateTime(_) => values
            .datetime_values
            .into_iter()
            .map(|v| Value::DateTime(v.into()))
            .collect(),
        ConcreteDataType::Date(_) => values
            .date_values
            .into_iter()
            .map(|v| Value::Date(v.into()))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Second(_)) => values
            .timestamp_second_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_second(v)))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => values
            .timestamp_millisecond_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_millisecond(v)))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => values
            .timestamp_microsecond_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_microsecond(v)))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => values
            .timestamp_nanosecond_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_nanosecond(v)))
            .collect(),
        ConcreteDataType::Time(TimeType::Second(_)) => values
            .time_second_values
            .into_iter()
            .map(|v| Value::Time(Time::new_second(v)))
            .collect(),
        ConcreteDataType::Time(TimeType::Millisecond(_)) => values
            .time_millisecond_values
            .into_iter()
            .map(|v| Value::Time(Time::new_millisecond(v)))
            .collect(),
        ConcreteDataType::Time(TimeType::Microsecond(_)) => values
            .time_microsecond_values
            .into_iter()
            .map(|v| Value::Time(Time::new_microsecond(v)))
            .collect(),
        ConcreteDataType::Time(TimeType::Nanosecond(_)) => values
            .time_nanosecond_values
            .into_iter()
            .map(|v| Value::Time(Time::new_nanosecond(v)))
            .collect(),

        ConcreteDataType::Interval(IntervalType::YearMonth(_)) => values
            .interval_year_month_values
            .into_iter()
            .map(|v| Value::Interval(Interval::from_i32(v)))
            .collect(),
        ConcreteDataType::Interval(IntervalType::DayTime(_)) => values
            .interval_day_time_values
            .into_iter()
            .map(|v| Value::Interval(Interval::from_i64(v)))
            .collect(),
        ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => values
            .interval_month_day_nano_values
            .into_iter()
            .map(|v| {
                Value::Interval(Interval::from_month_day_nano(
                    v.months,
                    v.days,
                    v.nanoseconds,
                ))
            })
            .collect(),
        ConcreteDataType::Duration(DurationType::Second(_)) => values
            .duration_second_values
            .into_iter()
            .map(|v| Value::Duration(Duration::new_second(v)))
            .collect(),
        ConcreteDataType::Duration(DurationType::Millisecond(_)) => values
            .duration_millisecond_values
            .into_iter()
            .map(|v| Value::Duration(Duration::new_millisecond(v)))
            .collect(),
        ConcreteDataType::Duration(DurationType::Microsecond(_)) => values
            .duration_microsecond_values
            .into_iter()
            .map(|v| Value::Duration(Duration::new_microsecond(v)))
            .collect(),
        ConcreteDataType::Duration(DurationType::Nanosecond(_)) => values
            .duration_nanosecond_values
            .into_iter()
            .map(|v| Value::Duration(Duration::new_nanosecond(v)))
            .collect(),
        ConcreteDataType::Decimal128(d) => values
            .decimal128_values
            .into_iter()
            .map(|v| {
                Value::Decimal128(Decimal128::from_value_precision_scale(
                    v.hi,
                    v.lo,
                    d.precision(),
                    d.scale(),
                ))
            })
            .collect(),
        ConcreteDataType::Null(_) | ConcreteDataType::List(_) | ConcreteDataType::Dictionary(_) => {
            unreachable!()
        }
    }
}

/// Returns true if the pb semantic type is valid.
pub fn is_semantic_type_eq(type_value: i32, semantic_type: SemanticType) -> bool {
    type_value == semantic_type as i32
}

/// Returns true if the pb type value is valid.
pub fn is_column_type_value_eq(
    type_value: i32,
    type_extension: Option<ColumnDataTypeExtension>,
    expect_type: &ConcreteDataType,
) -> bool {
    ColumnDataTypeWrapper::try_new(type_value, type_extension)
        .map(|wrapper| ConcreteDataType::from(wrapper) == *expect_type)
        .unwrap_or(false)
}

/// Convert value into proto's value.
pub fn to_proto_value(value: Value) -> Option<v1::Value> {
    let proto_value = match value {
        Value::Null => v1::Value { value_data: None },
        Value::Boolean(v) => v1::Value {
            value_data: Some(ValueData::BoolValue(v)),
        },
        Value::UInt8(v) => v1::Value {
            value_data: Some(ValueData::U8Value(v.into())),
        },
        Value::UInt16(v) => v1::Value {
            value_data: Some(ValueData::U16Value(v.into())),
        },
        Value::UInt32(v) => v1::Value {
            value_data: Some(ValueData::U32Value(v)),
        },
        Value::UInt64(v) => v1::Value {
            value_data: Some(ValueData::U64Value(v)),
        },
        Value::Int8(v) => v1::Value {
            value_data: Some(ValueData::I8Value(v.into())),
        },
        Value::Int16(v) => v1::Value {
            value_data: Some(ValueData::I16Value(v.into())),
        },
        Value::Int32(v) => v1::Value {
            value_data: Some(ValueData::I32Value(v)),
        },
        Value::Int64(v) => v1::Value {
            value_data: Some(ValueData::I64Value(v)),
        },
        Value::Float32(v) => v1::Value {
            value_data: Some(ValueData::F32Value(*v)),
        },
        Value::Float64(v) => v1::Value {
            value_data: Some(ValueData::F64Value(*v)),
        },
        Value::String(v) => v1::Value {
            value_data: Some(ValueData::StringValue(v.as_utf8().to_string())),
        },
        Value::Binary(v) => v1::Value {
            value_data: Some(ValueData::BinaryValue(v.to_vec())),
        },
        Value::Date(v) => v1::Value {
            value_data: Some(ValueData::DateValue(v.val())),
        },
        Value::DateTime(v) => v1::Value {
            value_data: Some(ValueData::DatetimeValue(v.val())),
        },
        Value::Timestamp(v) => match v.unit() {
            TimeUnit::Second => v1::Value {
                value_data: Some(ValueData::TimestampSecondValue(v.value())),
            },
            TimeUnit::Millisecond => v1::Value {
                value_data: Some(ValueData::TimestampMillisecondValue(v.value())),
            },
            TimeUnit::Microsecond => v1::Value {
                value_data: Some(ValueData::TimestampMicrosecondValue(v.value())),
            },
            TimeUnit::Nanosecond => v1::Value {
                value_data: Some(ValueData::TimestampNanosecondValue(v.value())),
            },
        },
        Value::Time(v) => match v.unit() {
            TimeUnit::Second => v1::Value {
                value_data: Some(ValueData::TimeSecondValue(v.value())),
            },
            TimeUnit::Millisecond => v1::Value {
                value_data: Some(ValueData::TimeMillisecondValue(v.value())),
            },
            TimeUnit::Microsecond => v1::Value {
                value_data: Some(ValueData::TimeMicrosecondValue(v.value())),
            },
            TimeUnit::Nanosecond => v1::Value {
                value_data: Some(ValueData::TimeNanosecondValue(v.value())),
            },
        },
        Value::Interval(v) => match v.unit() {
            IntervalUnit::YearMonth => v1::Value {
                value_data: Some(ValueData::IntervalYearMonthValue(v.to_i32())),
            },
            IntervalUnit::DayTime => v1::Value {
                value_data: Some(ValueData::IntervalDayTimeValue(v.to_i64())),
            },
            IntervalUnit::MonthDayNano => v1::Value {
                value_data: Some(ValueData::IntervalMonthDayNanoValue(
                    convert_i128_to_interval(v.to_i128()),
                )),
            },
        },
        Value::Duration(v) => match v.unit() {
            TimeUnit::Second => v1::Value {
                value_data: Some(ValueData::DurationSecondValue(v.value())),
            },
            TimeUnit::Millisecond => v1::Value {
                value_data: Some(ValueData::DurationMillisecondValue(v.value())),
            },
            TimeUnit::Microsecond => v1::Value {
                value_data: Some(ValueData::DurationMicrosecondValue(v.value())),
            },
            TimeUnit::Nanosecond => v1::Value {
                value_data: Some(ValueData::DurationNanosecondValue(v.value())),
            },
        },
        Value::Decimal128(v) => v1::Value {
            value_data: Some(ValueData::Decimal128Value(convert_to_pb_decimal128(v))),
        },
        Value::List(_) => return None,
    };

    Some(proto_value)
}

/// Returns the [ColumnDataTypeWrapper] of the value.
///
/// If value is null, returns `None`.
pub fn proto_value_type(value: &v1::Value) -> Option<ColumnDataType> {
    let value_type = match value.value_data.as_ref()? {
        ValueData::I8Value(_) => ColumnDataType::Int8,
        ValueData::I16Value(_) => ColumnDataType::Int16,
        ValueData::I32Value(_) => ColumnDataType::Int32,
        ValueData::I64Value(_) => ColumnDataType::Int64,
        ValueData::U8Value(_) => ColumnDataType::Uint8,
        ValueData::U16Value(_) => ColumnDataType::Uint16,
        ValueData::U32Value(_) => ColumnDataType::Uint32,
        ValueData::U64Value(_) => ColumnDataType::Uint64,
        ValueData::F32Value(_) => ColumnDataType::Float32,
        ValueData::F64Value(_) => ColumnDataType::Float64,
        ValueData::BoolValue(_) => ColumnDataType::Boolean,
        ValueData::BinaryValue(_) => ColumnDataType::Binary,
        ValueData::StringValue(_) => ColumnDataType::String,
        ValueData::DateValue(_) => ColumnDataType::Date,
        ValueData::DatetimeValue(_) => ColumnDataType::Datetime,
        ValueData::TimestampSecondValue(_) => ColumnDataType::TimestampSecond,
        ValueData::TimestampMillisecondValue(_) => ColumnDataType::TimestampMillisecond,
        ValueData::TimestampMicrosecondValue(_) => ColumnDataType::TimestampMicrosecond,
        ValueData::TimestampNanosecondValue(_) => ColumnDataType::TimestampNanosecond,
        ValueData::TimeSecondValue(_) => ColumnDataType::TimeSecond,
        ValueData::TimeMillisecondValue(_) => ColumnDataType::TimeMillisecond,
        ValueData::TimeMicrosecondValue(_) => ColumnDataType::TimeMicrosecond,
        ValueData::TimeNanosecondValue(_) => ColumnDataType::TimeNanosecond,
        ValueData::IntervalYearMonthValue(_) => ColumnDataType::IntervalYearMonth,
        ValueData::IntervalDayTimeValue(_) => ColumnDataType::IntervalDayTime,
        ValueData::IntervalMonthDayNanoValue(_) => ColumnDataType::IntervalMonthDayNano,
        ValueData::DurationSecondValue(_) => ColumnDataType::DurationSecond,
        ValueData::DurationMillisecondValue(_) => ColumnDataType::DurationMillisecond,
        ValueData::DurationMicrosecondValue(_) => ColumnDataType::DurationMicrosecond,
        ValueData::DurationNanosecondValue(_) => ColumnDataType::DurationNanosecond,
        ValueData::Decimal128Value(_) => ColumnDataType::Decimal128,
    };
    Some(value_type)
}

pub fn vectors_to_rows<'a>(
    columns: impl Iterator<Item = &'a VectorRef>,
    row_count: usize,
) -> Vec<Row> {
    let mut rows = vec![Row { values: vec![] }; row_count];
    for column in columns {
        for (row_index, row) in rows.iter_mut().enumerate() {
            row.values.push(value_to_grpc_value(column.get(row_index)))
        }
    }

    rows
}

pub fn value_to_grpc_value(value: Value) -> GrpcValue {
    GrpcValue {
        value_data: match value {
            Value::Null => None,
            Value::Boolean(v) => Some(ValueData::BoolValue(v)),
            Value::UInt8(v) => Some(ValueData::U8Value(v as _)),
            Value::UInt16(v) => Some(ValueData::U16Value(v as _)),
            Value::UInt32(v) => Some(ValueData::U32Value(v)),
            Value::UInt64(v) => Some(ValueData::U64Value(v)),
            Value::Int8(v) => Some(ValueData::I8Value(v as _)),
            Value::Int16(v) => Some(ValueData::I16Value(v as _)),
            Value::Int32(v) => Some(ValueData::I32Value(v)),
            Value::Int64(v) => Some(ValueData::I64Value(v)),
            Value::Float32(v) => Some(ValueData::F32Value(*v)),
            Value::Float64(v) => Some(ValueData::F64Value(*v)),
            Value::String(v) => Some(ValueData::StringValue(v.as_utf8().to_string())),
            Value::Binary(v) => Some(ValueData::BinaryValue(v.to_vec())),
            Value::Date(v) => Some(ValueData::DateValue(v.val())),
            Value::DateTime(v) => Some(ValueData::DatetimeValue(v.val())),
            Value::Timestamp(v) => Some(match v.unit() {
                TimeUnit::Second => ValueData::TimestampSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::TimestampMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::TimestampMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::TimestampNanosecondValue(v.value()),
            }),
            Value::Time(v) => Some(match v.unit() {
                TimeUnit::Second => ValueData::TimeSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::TimeMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::TimeMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::TimeNanosecondValue(v.value()),
            }),
            Value::Interval(v) => Some(match v.unit() {
                IntervalUnit::YearMonth => ValueData::IntervalYearMonthValue(v.to_i32()),
                IntervalUnit::DayTime => ValueData::IntervalDayTimeValue(v.to_i64()),
                IntervalUnit::MonthDayNano => {
                    ValueData::IntervalMonthDayNanoValue(convert_i128_to_interval(v.to_i128()))
                }
            }),
            Value::Duration(v) => Some(match v.unit() {
                TimeUnit::Second => ValueData::DurationSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::DurationMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::DurationMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::DurationNanosecondValue(v.value()),
            }),
            Value::Decimal128(v) => Some(ValueData::Decimal128Value(convert_to_pb_decimal128(v))),
            Value::List(_) => unreachable!(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::types::{
        DurationMillisecondType, DurationSecondType, Int32Type, IntervalDayTimeType,
        IntervalMonthDayNanoType, IntervalYearMonthType, TimeMillisecondType, TimeSecondType,
        TimestampMillisecondType, TimestampSecondType, UInt32Type,
    };
    use datatypes::vectors::{
        BooleanVector, DurationMicrosecondVector, DurationMillisecondVector,
        DurationNanosecondVector, DurationSecondVector, IntervalDayTimeVector,
        IntervalMonthDayNanoVector, IntervalYearMonthVector, TimeMicrosecondVector,
        TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector, TimestampMicrosecondVector,
        TimestampMillisecondVector, TimestampNanosecondVector, TimestampSecondVector, Vector,
    };
    use paste::paste;

    use super::*;

    #[test]
    fn test_values_with_capacity() {
        let values = values_with_capacity(ColumnDataType::Int8, 2);
        let values = values.i8_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Int32, 2);
        let values = values.i32_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Int64, 2);
        let values = values.i64_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Uint8, 2);
        let values = values.u8_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Uint32, 2);
        let values = values.u32_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Uint64, 2);
        let values = values.u64_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Float32, 2);
        let values = values.f32_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Float64, 2);
        let values = values.f64_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Binary, 2);
        let values = values.binary_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Boolean, 2);
        let values = values.bool_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::String, 2);
        let values = values.string_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Date, 2);
        let values = values.date_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Datetime, 2);
        let values = values.datetime_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::TimestampMillisecond, 2);
        let values = values.timestamp_millisecond_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::TimeMillisecond, 2);
        let values = values.time_millisecond_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::IntervalDayTime, 2);
        let values = values.interval_day_time_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::IntervalMonthDayNano, 2);
        let values = values.interval_month_day_nano_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::DurationMillisecond, 2);
        let values = values.duration_millisecond_values;
        assert_eq!(2, values.capacity());

        let values = values_with_capacity(ColumnDataType::Decimal128, 2);
        let values = values.decimal128_values;
        assert_eq!(2, values.capacity());
    }

    #[test]
    fn test_concrete_datatype_from_column_datatype() {
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            ColumnDataTypeWrapper::boolean_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int8_datatype(),
            ColumnDataTypeWrapper::int8_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int16_datatype(),
            ColumnDataTypeWrapper::int16_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int32_datatype(),
            ColumnDataTypeWrapper::int32_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            ColumnDataTypeWrapper::int64_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint8_datatype(),
            ColumnDataTypeWrapper::uint8_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint16_datatype(),
            ColumnDataTypeWrapper::uint16_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint32_datatype(),
            ColumnDataTypeWrapper::uint32_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            ColumnDataTypeWrapper::uint64_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::float32_datatype(),
            ColumnDataTypeWrapper::float32_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ColumnDataTypeWrapper::float64_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::binary_datatype(),
            ColumnDataTypeWrapper::binary_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            ColumnDataTypeWrapper::string_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::date_datatype(),
            ColumnDataTypeWrapper::date_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::datetime_datatype(),
            ColumnDataTypeWrapper::datetime_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            ColumnDataTypeWrapper::timestamp_millisecond_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ColumnDataTypeWrapper::time_millisecond_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime),
            ColumnDataTypeWrapper::interval_day_time_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth),
            ColumnDataTypeWrapper::interval_year_month_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ColumnDataTypeWrapper::interval_month_day_nano_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::duration_millisecond_datatype(),
            ColumnDataTypeWrapper::duration_millisecond_datatype().into()
        );
        assert_eq!(
            ConcreteDataType::decimal128_datatype(10, 2),
            ColumnDataTypeWrapper::decimal128_datatype(10, 2).into()
        )
    }

    #[test]
    fn test_column_datatype_from_concrete_datatype() {
        assert_eq!(
            ColumnDataTypeWrapper::boolean_datatype(),
            ConcreteDataType::boolean_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int8_datatype(),
            ConcreteDataType::int8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int16_datatype(),
            ConcreteDataType::int16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int32_datatype(),
            ConcreteDataType::int32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::int64_datatype(),
            ConcreteDataType::int64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint8_datatype(),
            ConcreteDataType::uint8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint16_datatype(),
            ConcreteDataType::uint16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint32_datatype(),
            ConcreteDataType::uint32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::uint64_datatype(),
            ConcreteDataType::uint64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::float32_datatype(),
            ConcreteDataType::float32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::float64_datatype(),
            ConcreteDataType::float64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::binary_datatype(),
            ConcreteDataType::binary_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::string_datatype(),
            ConcreteDataType::string_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::date_datatype(),
            ConcreteDataType::date_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::datetime_datatype(),
            ConcreteDataType::datetime_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::timestamp_millisecond_datatype(),
            ConcreteDataType::timestamp_millisecond_datatype()
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::interval_year_month_datatype(),
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::interval_day_time_datatype(),
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::interval_month_day_nano_datatype(),
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper::duration_millisecond_datatype(),
            ConcreteDataType::duration_millisecond_datatype()
                .try_into()
                .unwrap()
        );

        assert_eq!(
            ColumnDataTypeWrapper::decimal128_datatype(10, 2),
            ConcreteDataType::decimal128_datatype(10, 2)
                .try_into()
                .unwrap()
        );

        let result: Result<ColumnDataTypeWrapper> = ConcreteDataType::null_datatype().try_into();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to create column datatype from Null(NullType)"
        );

        let result: Result<ColumnDataTypeWrapper> =
            ConcreteDataType::list_datatype(ConcreteDataType::boolean_datatype()).try_into();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            "Failed to create column datatype from List(ListType { item_type: Boolean(BooleanType) })"
        );
    }

    #[test]
    fn test_column_put_timestamp_values() {
        let mut column = Column {
            column_name: "test".to_string(),
            semantic_type: 0,
            values: Some(Values {
                ..Default::default()
            }),
            null_mask: vec![],
            datatype: 0,
            ..Default::default()
        };

        let vector = Arc::new(TimestampNanosecondVector::from_vec(vec![1, 2, 3]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![1, 2, 3],
            column.values.as_ref().unwrap().timestamp_nanosecond_values
        );

        let vector = Arc::new(TimestampMillisecondVector::from_vec(vec![4, 5, 6]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![4, 5, 6],
            column.values.as_ref().unwrap().timestamp_millisecond_values
        );

        let vector = Arc::new(TimestampMicrosecondVector::from_vec(vec![7, 8, 9]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![7, 8, 9],
            column.values.as_ref().unwrap().timestamp_microsecond_values
        );

        let vector = Arc::new(TimestampSecondVector::from_vec(vec![10, 11, 12]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![10, 11, 12],
            column.values.as_ref().unwrap().timestamp_second_values
        );
    }

    #[test]
    fn test_column_put_time_values() {
        let mut column = Column {
            column_name: "test".to_string(),
            semantic_type: 0,
            values: Some(Values {
                ..Default::default()
            }),
            null_mask: vec![],
            datatype: 0,
            ..Default::default()
        };

        let vector = Arc::new(TimeNanosecondVector::from_vec(vec![1, 2, 3]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![1, 2, 3],
            column.values.as_ref().unwrap().time_nanosecond_values
        );

        let vector = Arc::new(TimeMillisecondVector::from_vec(vec![4, 5, 6]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![4, 5, 6],
            column.values.as_ref().unwrap().time_millisecond_values
        );

        let vector = Arc::new(TimeMicrosecondVector::from_vec(vec![7, 8, 9]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![7, 8, 9],
            column.values.as_ref().unwrap().time_microsecond_values
        );

        let vector = Arc::new(TimeSecondVector::from_vec(vec![10, 11, 12]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![10, 11, 12],
            column.values.as_ref().unwrap().time_second_values
        );
    }

    #[test]
    fn test_column_put_interval_values() {
        let mut column = Column {
            column_name: "test".to_string(),
            semantic_type: 0,
            values: Some(Values {
                ..Default::default()
            }),
            null_mask: vec![],
            datatype: 0,
            ..Default::default()
        };

        let vector = Arc::new(IntervalYearMonthVector::from_vec(vec![1, 2, 3]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![1, 2, 3],
            column.values.as_ref().unwrap().interval_year_month_values
        );

        let vector = Arc::new(IntervalDayTimeVector::from_vec(vec![4, 5, 6]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![4, 5, 6],
            column.values.as_ref().unwrap().interval_day_time_values
        );

        let vector = Arc::new(IntervalMonthDayNanoVector::from_vec(vec![7, 8, 9]));
        let len = vector.len();
        push_vals(&mut column, 3, vector);
        (0..len).for_each(|i| {
            assert_eq!(
                7 + i as i64,
                column
                    .values
                    .as_ref()
                    .unwrap()
                    .interval_month_day_nano_values
                    .get(i)
                    .unwrap()
                    .nanoseconds
            );
        });
    }

    #[test]
    fn test_column_put_duration_values() {
        let mut column = Column {
            column_name: "test".to_string(),
            semantic_type: 0,
            values: Some(Values {
                ..Default::default()
            }),
            null_mask: vec![],
            datatype: 0,
            ..Default::default()
        };

        let vector = Arc::new(DurationNanosecondVector::from_vec(vec![1, 2, 3]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![1, 2, 3],
            column.values.as_ref().unwrap().duration_nanosecond_values
        );

        let vector = Arc::new(DurationMicrosecondVector::from_vec(vec![7, 8, 9]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![7, 8, 9],
            column.values.as_ref().unwrap().duration_microsecond_values
        );

        let vector = Arc::new(DurationMillisecondVector::from_vec(vec![4, 5, 6]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![4, 5, 6],
            column.values.as_ref().unwrap().duration_millisecond_values
        );

        let vector = Arc::new(DurationSecondVector::from_vec(vec![10, 11, 12]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![10, 11, 12],
            column.values.as_ref().unwrap().duration_second_values
        );
    }

    #[test]
    fn test_column_put_vector() {
        use crate::v1::SemanticType;
        // Some(false), None, Some(true), Some(true)
        let mut column = Column {
            column_name: "test".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(Values {
                bool_values: vec![false, true, true],
                ..Default::default()
            }),
            null_mask: vec![2],
            datatype: ColumnDataType::Boolean as i32,
            ..Default::default()
        };
        let row_count = 4;

        let vector = Arc::new(BooleanVector::from(vec![Some(true), None, Some(false)]));
        push_vals(&mut column, row_count, vector);
        // Some(false), None, Some(true), Some(true), Some(true), None, Some(false)
        let bool_values = column.values.unwrap().bool_values;
        assert_eq!(vec![false, true, true, true, false], bool_values);
        let null_mask = column.null_mask;
        assert_eq!(34, null_mask[0]);
    }

    #[test]
    fn test_convert_i128_to_interval() {
        let i128_val = 3000;
        let interval = convert_i128_to_interval(i128_val);
        assert_eq!(interval.months, 0);
        assert_eq!(interval.days, 0);
        assert_eq!(interval.nanoseconds, 3000);
    }

    #[test]
    fn test_convert_timestamp_values() {
        // second
        let actual = pb_values_to_values(
            &ConcreteDataType::Timestamp(TimestampType::Second(TimestampSecondType)),
            Values {
                timestamp_second_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Timestamp(Timestamp::new_second(1_i64)),
            Value::Timestamp(Timestamp::new_second(2_i64)),
            Value::Timestamp(Timestamp::new_second(3_i64)),
        ];
        assert_eq!(expect, actual);

        // millisecond
        let actual = pb_values_to_values(
            &ConcreteDataType::Timestamp(TimestampType::Millisecond(TimestampMillisecondType)),
            Values {
                timestamp_millisecond_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Timestamp(Timestamp::new_millisecond(1_i64)),
            Value::Timestamp(Timestamp::new_millisecond(2_i64)),
            Value::Timestamp(Timestamp::new_millisecond(3_i64)),
        ];
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_convert_time_values() {
        // second
        let actual = pb_values_to_values(
            &ConcreteDataType::Time(TimeType::Second(TimeSecondType)),
            Values {
                time_second_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Time(Time::new_second(1_i64)),
            Value::Time(Time::new_second(2_i64)),
            Value::Time(Time::new_second(3_i64)),
        ];
        assert_eq!(expect, actual);

        // millisecond
        let actual = pb_values_to_values(
            &ConcreteDataType::Time(TimeType::Millisecond(TimeMillisecondType)),
            Values {
                time_millisecond_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Time(Time::new_millisecond(1_i64)),
            Value::Time(Time::new_millisecond(2_i64)),
            Value::Time(Time::new_millisecond(3_i64)),
        ];
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_convert_duration_values() {
        // second
        let actual = pb_values_to_values(
            &ConcreteDataType::Duration(DurationType::Second(DurationSecondType)),
            Values {
                duration_second_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Duration(Duration::new_second(1_i64)),
            Value::Duration(Duration::new_second(2_i64)),
            Value::Duration(Duration::new_second(3_i64)),
        ];
        assert_eq!(expect, actual);

        // millisecond
        let actual = pb_values_to_values(
            &ConcreteDataType::Duration(DurationType::Millisecond(DurationMillisecondType)),
            Values {
                duration_millisecond_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Duration(Duration::new_millisecond(1_i64)),
            Value::Duration(Duration::new_millisecond(2_i64)),
            Value::Duration(Duration::new_millisecond(3_i64)),
        ];
        assert_eq!(expect, actual);
    }

    #[test]
    fn test_convert_interval_values() {
        // year_month
        let actual = pb_values_to_values(
            &ConcreteDataType::Interval(IntervalType::YearMonth(IntervalYearMonthType)),
            Values {
                interval_year_month_values: vec![1_i32, 2_i32, 3_i32],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Interval(Interval::from_year_month(1_i32)),
            Value::Interval(Interval::from_year_month(2_i32)),
            Value::Interval(Interval::from_year_month(3_i32)),
        ];
        assert_eq!(expect, actual);

        // day_time
        let actual = pb_values_to_values(
            &ConcreteDataType::Interval(IntervalType::DayTime(IntervalDayTimeType)),
            Values {
                interval_day_time_values: vec![1_i64, 2_i64, 3_i64],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Interval(Interval::from_i64(1_i64)),
            Value::Interval(Interval::from_i64(2_i64)),
            Value::Interval(Interval::from_i64(3_i64)),
        ];
        assert_eq!(expect, actual);

        // month_day_nano
        let actual = pb_values_to_values(
            &ConcreteDataType::Interval(IntervalType::MonthDayNano(IntervalMonthDayNanoType)),
            Values {
                interval_month_day_nano_values: vec![
                    v1::IntervalMonthDayNano {
                        months: 1,
                        days: 2,
                        nanoseconds: 3,
                    },
                    v1::IntervalMonthDayNano {
                        months: 5,
                        days: 6,
                        nanoseconds: 7,
                    },
                    v1::IntervalMonthDayNano {
                        months: 9,
                        days: 10,
                        nanoseconds: 11,
                    },
                ],
                ..Default::default()
            },
        );
        let expect = vec![
            Value::Interval(Interval::from_month_day_nano(1, 2, 3)),
            Value::Interval(Interval::from_month_day_nano(5, 6, 7)),
            Value::Interval(Interval::from_month_day_nano(9, 10, 11)),
        ];
        assert_eq!(expect, actual);
    }

    macro_rules! test_convert_values {
        ($grpc_data_type: ident, $values: expr,  $concrete_data_type: ident, $expected_ret: expr) => {
            paste! {
                #[test]
                fn [<test_convert_ $grpc_data_type _values>]() {
                    let values = Values {
                        [<$grpc_data_type _values>]: $values,
                        ..Default::default()
                    };

                    let data_type = ConcreteDataType::[<$concrete_data_type _datatype>]();
                    let result = pb_values_to_values(&data_type, values);

                    assert_eq!(
                        $expected_ret,
                        result
                    );
                }
            }
        };
    }

    test_convert_values!(
        i8,
        vec![1_i32, 2, 3],
        int8,
        vec![Value::Int8(1), Value::Int8(2), Value::Int8(3)]
    );

    test_convert_values!(
        u8,
        vec![1_u32, 2, 3],
        uint8,
        vec![Value::UInt8(1), Value::UInt8(2), Value::UInt8(3)]
    );

    test_convert_values!(
        i16,
        vec![1_i32, 2, 3],
        int16,
        vec![Value::Int16(1), Value::Int16(2), Value::Int16(3)]
    );

    test_convert_values!(
        u16,
        vec![1_u32, 2, 3],
        uint16,
        vec![Value::UInt16(1), Value::UInt16(2), Value::UInt16(3)]
    );

    test_convert_values!(
        i32,
        vec![1, 2, 3],
        int32,
        vec![Value::Int32(1), Value::Int32(2), Value::Int32(3)]
    );

    test_convert_values!(
        u32,
        vec![1, 2, 3],
        uint32,
        vec![Value::UInt32(1), Value::UInt32(2), Value::UInt32(3)]
    );

    test_convert_values!(
        i64,
        vec![1, 2, 3],
        int64,
        vec![Value::Int64(1), Value::Int64(2), Value::Int64(3)]
    );

    test_convert_values!(
        u64,
        vec![1, 2, 3],
        uint64,
        vec![Value::UInt64(1), Value::UInt64(2), Value::UInt64(3)]
    );

    test_convert_values!(
        f32,
        vec![1.0, 2.0, 3.0],
        float32,
        vec![
            Value::Float32(1.0.into()),
            Value::Float32(2.0.into()),
            Value::Float32(3.0.into())
        ]
    );

    test_convert_values!(
        f64,
        vec![1.0, 2.0, 3.0],
        float64,
        vec![
            Value::Float64(1.0.into()),
            Value::Float64(2.0.into()),
            Value::Float64(3.0.into())
        ]
    );

    test_convert_values!(
        string,
        vec!["1".to_string(), "2".to_string(), "3".to_string()],
        string,
        vec![
            Value::String("1".into()),
            Value::String("2".into()),
            Value::String("3".into())
        ]
    );

    test_convert_values!(
        binary,
        vec!["1".into(), "2".into(), "3".into()],
        binary,
        vec![
            Value::Binary(b"1".to_vec().into()),
            Value::Binary(b"2".to_vec().into()),
            Value::Binary(b"3".to_vec().into())
        ]
    );

    test_convert_values!(
        date,
        vec![1, 2, 3],
        date,
        vec![
            Value::Date(1.into()),
            Value::Date(2.into()),
            Value::Date(3.into())
        ]
    );

    test_convert_values!(
        datetime,
        vec![1.into(), 2.into(), 3.into()],
        datetime,
        vec![
            Value::DateTime(1.into()),
            Value::DateTime(2.into()),
            Value::DateTime(3.into())
        ]
    );

    #[test]
    fn test_vectors_to_rows_for_different_types() {
        let boolean_vec = BooleanVector::from_vec(vec![true, false, true]);
        let int8_vec = PrimitiveVector::<Int8Type>::from_iter_values(vec![1, 2, 3]);
        let int32_vec = PrimitiveVector::<Int32Type>::from_iter_values(vec![100, 200, 300]);
        let uint8_vec = PrimitiveVector::<UInt8Type>::from_iter_values(vec![10, 20, 30]);
        let uint32_vec = PrimitiveVector::<UInt32Type>::from_iter_values(vec![1000, 2000, 3000]);
        let float32_vec = Float32Vector::from_vec(vec![1.1, 2.2, 3.3]);
        let date_vec = DateVector::from_vec(vec![10, 20, 30]);
        let string_vec = StringVector::from_vec(vec!["a", "b", "c"]);

        let vector_refs: Vec<VectorRef> = vec![
            Arc::new(boolean_vec),
            Arc::new(int8_vec),
            Arc::new(int32_vec),
            Arc::new(uint8_vec),
            Arc::new(uint32_vec),
            Arc::new(float32_vec),
            Arc::new(date_vec),
            Arc::new(string_vec),
        ];

        let result = vectors_to_rows(vector_refs.iter(), 3);

        assert_eq!(result.len(), 3);

        assert_eq!(result[0].values.len(), 8);
        let values = result[0]
            .values
            .iter()
            .map(|v| v.value_data.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0], ValueData::BoolValue(true));
        assert_eq!(values[1], ValueData::I8Value(1));
        assert_eq!(values[2], ValueData::I32Value(100));
        assert_eq!(values[3], ValueData::U8Value(10));
        assert_eq!(values[4], ValueData::U32Value(1000));
        assert_eq!(values[5], ValueData::F32Value(1.1));
        assert_eq!(values[6], ValueData::DateValue(10));
        assert_eq!(values[7], ValueData::StringValue("a".to_string()));

        assert_eq!(result[1].values.len(), 8);
        let values = result[1]
            .values
            .iter()
            .map(|v| v.value_data.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0], ValueData::BoolValue(false));
        assert_eq!(values[1], ValueData::I8Value(2));
        assert_eq!(values[2], ValueData::I32Value(200));
        assert_eq!(values[3], ValueData::U8Value(20));
        assert_eq!(values[4], ValueData::U32Value(2000));
        assert_eq!(values[5], ValueData::F32Value(2.2));
        assert_eq!(values[6], ValueData::DateValue(20));
        assert_eq!(values[7], ValueData::StringValue("b".to_string()));

        assert_eq!(result[2].values.len(), 8);
        let values = result[2]
            .values
            .iter()
            .map(|v| v.value_data.clone().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(values[0], ValueData::BoolValue(true));
        assert_eq!(values[1], ValueData::I8Value(3));
        assert_eq!(values[2], ValueData::I32Value(300));
        assert_eq!(values[3], ValueData::U8Value(30));
        assert_eq!(values[4], ValueData::U32Value(3000));
        assert_eq!(values[5], ValueData::F32Value(3.3));
        assert_eq!(values[6], ValueData::DateValue(30));
        assert_eq!(values[7], ValueData::StringValue("c".to_string()));
    }

    #[test]
    fn test_is_column_type_value_eq() {
        // test column type eq
        let column1 = Column {
            column_name: "test".to_string(),
            semantic_type: 0,
            values: Some(Values {
                bool_values: vec![false, true, true],
                ..Default::default()
            }),
            null_mask: vec![2],
            datatype: ColumnDataType::Boolean as i32,
            datatype_extension: None,
        };
        assert!(is_column_type_value_eq(
            column1.datatype,
            column1.datatype_extension,
            &ConcreteDataType::boolean_datatype(),
        ));
    }

    #[test]
    fn test_convert_to_pb_decimal128() {
        let decimal = Decimal128::new(123, 3, 1);
        let pb_decimal = convert_to_pb_decimal128(decimal);
        assert_eq!(pb_decimal.lo, 123);
        assert_eq!(pb_decimal.hi, 0);
    }
}
