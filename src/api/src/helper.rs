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
use common_time::interval::IntervalUnit;
use common_time::time::Time;
use common_time::timestamp::TimeUnit;
use common_time::{Date, DateTime, Interval, Timestamp};
use datatypes::prelude::{ConcreteDataType, ValueRef};
use datatypes::scalars::ScalarVector;
use datatypes::types::{
    Int16Type, Int8Type, IntervalType, TimeType, TimestampType, UInt16Type, UInt8Type,
};
use datatypes::value::{OrderedF32, OrderedF64, Value};
use datatypes::vectors::{
    BinaryVector, BooleanVector, DateTimeVector, DateVector, Float32Vector, Float64Vector,
    Int32Vector, Int64Vector, IntervalDayTimeVector, IntervalMonthDayNanoVector,
    IntervalYearMonthVector, PrimitiveVector, StringVector, TimeMicrosecondVector,
    TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector, TimestampMicrosecondVector,
    TimestampMillisecondVector, TimestampNanosecondVector, TimestampSecondVector, UInt32Vector,
    UInt64Vector, VectorRef,
};
use greptime_proto::v1::ddl_request::Expr;
use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::query_request::Query;
use greptime_proto::v1::value::ValueData;
use greptime_proto::v1::{self, DdlRequest, IntervalMonthDayNano, QueryRequest, Row, SemanticType};
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::v1::column::Values;
use crate::v1::{Column, ColumnDataType, Value as GrpcValue};

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnDataTypeWrapper(ColumnDataType);

impl ColumnDataTypeWrapper {
    pub fn try_new(datatype: i32) -> Result<Self> {
        let datatype = ColumnDataType::from_i32(datatype)
            .context(error::UnknownColumnDataTypeSnafu { datatype })?;
        Ok(Self(datatype))
    }

    pub fn new(datatype: ColumnDataType) -> Self {
        Self(datatype)
    }

    pub fn datatype(&self) -> ColumnDataType {
        self.0
    }
}

impl From<ColumnDataTypeWrapper> for ConcreteDataType {
    fn from(datatype: ColumnDataTypeWrapper) -> Self {
        match datatype.0 {
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
        }
    }
}

impl TryFrom<ConcreteDataType> for ColumnDataTypeWrapper {
    type Error = error::Error;

    fn try_from(datatype: ConcreteDataType) -> Result<Self> {
        let datatype = ColumnDataTypeWrapper(match datatype {
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
            ConcreteDataType::Null(_)
            | ConcreteDataType::List(_)
            | ConcreteDataType::Dictionary(_) => {
                return error::IntoColumnDataTypeSnafu { from: datatype }.fail()
            }
        });
        Ok(datatype)
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
            ts_second_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampMillisecond => Values {
            ts_millisecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampMicrosecond => Values {
            ts_microsecond_values: Vec::with_capacity(capacity),
            ..Default::default()
        },
        ColumnDataType::TimestampNanosecond => Values {
            ts_nanosecond_values: Vec::with_capacity(capacity),
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
            TimeUnit::Second => values.ts_second_values.push(val.value()),
            TimeUnit::Millisecond => values.ts_millisecond_values.push(val.value()),
            TimeUnit::Microsecond => values.ts_microsecond_values.push(val.value()),
            TimeUnit::Nanosecond => values.ts_nanosecond_values.push(val.value()),
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
pub fn convert_i128_to_interval(v: i128) -> IntervalMonthDayNano {
    let interval = Interval::from_i128(v);
    let (months, days, nanoseconds) = interval.to_month_day_nano();
    IntervalMonthDayNano {
        months,
        days,
        nanoseconds,
    }
}

pub fn pb_value_to_value_ref(value: &v1::Value) -> ValueRef {
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
        ValueData::TsSecondValue(t) => ValueRef::Timestamp(Timestamp::new_second(*t)),
        ValueData::TsMillisecondValue(t) => ValueRef::Timestamp(Timestamp::new_millisecond(*t)),
        ValueData::TsMicrosecondValue(t) => ValueRef::Timestamp(Timestamp::new_microsecond(*t)),
        ValueData::TsNanosecondValue(t) => ValueRef::Timestamp(Timestamp::new_nanosecond(*t)),
        ValueData::TimeSecondValue(t) => ValueRef::Time(Time::new_second(*t)),
        ValueData::TimeMillisecondValue(t) => ValueRef::Time(Time::new_millisecond(*t)),
        ValueData::TimeMicrosecondValue(t) => ValueRef::Time(Time::new_microsecond(*t)),
        ValueData::TimeNanosecondValue(t) => ValueRef::Time(Time::new_nanosecond(*t)),
        ValueData::IntervalYearMonthValues(v) => ValueRef::Interval(Interval::from_i32(*v)),
        ValueData::IntervalDayTimeValues(v) => ValueRef::Interval(Interval::from_i64(*v)),
        ValueData::IntervalMonthDayNanoValues(v) => {
            let interval = Interval::from_month_day_nano(v.months, v.days, v.nanoseconds);
            ValueRef::Interval(interval)
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
            TimestampType::Second(_) => {
                Arc::new(TimestampSecondVector::from_vec(values.ts_second_values))
            }
            TimestampType::Millisecond(_) => Arc::new(TimestampMillisecondVector::from_vec(
                values.ts_millisecond_values,
            )),
            TimestampType::Microsecond(_) => Arc::new(TimestampMicrosecondVector::from_vec(
                values.ts_microsecond_values,
            )),
            TimestampType::Nanosecond(_) => Arc::new(TimestampNanosecondVector::from_vec(
                values.ts_nanosecond_values,
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
            .ts_second_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_second(v)))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => values
            .ts_millisecond_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_millisecond(v)))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => values
            .ts_microsecond_values
            .into_iter()
            .map(|v| Value::Timestamp(Timestamp::new_microsecond(v)))
            .collect(),
        ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => values
            .ts_nanosecond_values
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
pub fn is_column_type_value_eq(type_value: i32, expect_type: &ConcreteDataType) -> bool {
    let Some(column_type) = ColumnDataType::from_i32(type_value) else {
        return false;
    };

    is_column_type_eq(column_type, expect_type)
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
                value_data: Some(ValueData::TsSecondValue(v.value())),
            },
            TimeUnit::Millisecond => v1::Value {
                value_data: Some(ValueData::TsMillisecondValue(v.value())),
            },
            TimeUnit::Microsecond => v1::Value {
                value_data: Some(ValueData::TsMicrosecondValue(v.value())),
            },
            TimeUnit::Nanosecond => v1::Value {
                value_data: Some(ValueData::TsNanosecondValue(v.value())),
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
                value_data: Some(ValueData::IntervalYearMonthValues(v.to_i32())),
            },
            IntervalUnit::DayTime => v1::Value {
                value_data: Some(ValueData::IntervalDayTimeValues(v.to_i64())),
            },
            IntervalUnit::MonthDayNano => v1::Value {
                value_data: Some(ValueData::IntervalMonthDayNanoValues(
                    convert_i128_to_interval(v.to_i128()),
                )),
            },
        },
        Value::List(_) => return None,
    };

    Some(proto_value)
}

/// Returns the [ColumnDataType] of the value.
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
        ValueData::TsSecondValue(_) => ColumnDataType::TimestampSecond,
        ValueData::TsMillisecondValue(_) => ColumnDataType::TimestampMillisecond,
        ValueData::TsMicrosecondValue(_) => ColumnDataType::TimestampMicrosecond,
        ValueData::TsNanosecondValue(_) => ColumnDataType::TimestampNanosecond,
        ValueData::TimeSecondValue(_) => ColumnDataType::TimeSecond,
        ValueData::TimeMillisecondValue(_) => ColumnDataType::TimeMillisecond,
        ValueData::TimeMicrosecondValue(_) => ColumnDataType::TimeMicrosecond,
        ValueData::TimeNanosecondValue(_) => ColumnDataType::TimeNanosecond,
        ValueData::IntervalYearMonthValues(_) => ColumnDataType::IntervalYearMonth,
        ValueData::IntervalDayTimeValues(_) => ColumnDataType::IntervalDayTime,
        ValueData::IntervalMonthDayNanoValues(_) => ColumnDataType::IntervalMonthDayNano,
    };
    Some(value_type)
}

/// Convert [ConcreteDataType] to [ColumnDataType].
pub fn to_column_data_type(data_type: &ConcreteDataType) -> Option<ColumnDataType> {
    let column_data_type = match data_type {
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
        ConcreteDataType::Timestamp(TimestampType::Second(_)) => ColumnDataType::TimestampSecond,
        ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => {
            ColumnDataType::TimestampMillisecond
        }
        ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => {
            ColumnDataType::TimestampMicrosecond
        }
        ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => {
            ColumnDataType::TimestampNanosecond
        }
        ConcreteDataType::Time(TimeType::Second(_)) => ColumnDataType::TimeSecond,
        ConcreteDataType::Time(TimeType::Millisecond(_)) => ColumnDataType::TimeMillisecond,
        ConcreteDataType::Time(TimeType::Microsecond(_)) => ColumnDataType::TimeMicrosecond,
        ConcreteDataType::Time(TimeType::Nanosecond(_)) => ColumnDataType::TimeNanosecond,
        ConcreteDataType::Null(_)
        | ConcreteDataType::Interval(_)
        | ConcreteDataType::List(_)
        | ConcreteDataType::Dictionary(_) => return None,
    };

    Some(column_data_type)
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
                TimeUnit::Second => ValueData::TsSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::TsMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::TsMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::TsNanosecondValue(v.value()),
            }),
            Value::Time(v) => Some(match v.unit() {
                TimeUnit::Second => ValueData::TimeSecondValue(v.value()),
                TimeUnit::Millisecond => ValueData::TimeMillisecondValue(v.value()),
                TimeUnit::Microsecond => ValueData::TimeMicrosecondValue(v.value()),
                TimeUnit::Nanosecond => ValueData::TimeNanosecondValue(v.value()),
            }),
            Value::Interval(v) => Some(match v.unit() {
                IntervalUnit::YearMonth => ValueData::IntervalYearMonthValues(v.to_i32()),
                IntervalUnit::DayTime => ValueData::IntervalDayTimeValues(v.to_i64()),
                IntervalUnit::MonthDayNano => {
                    ValueData::IntervalMonthDayNanoValues(convert_i128_to_interval(v.to_i128()))
                }
            }),
            Value::List(_) => unreachable!(),
        },
    }
}

/// Returns true if the column type is equal to expected type.
fn is_column_type_eq(column_type: ColumnDataType, expect_type: &ConcreteDataType) -> bool {
    if let Some(expect) = to_column_data_type(expect_type) {
        column_type == expect
    } else {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::types::{
        Int32Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
        TimeMillisecondType, TimeSecondType, TimestampMillisecondType, TimestampSecondType,
        UInt32Type,
    };
    use datatypes::vectors::{
        BooleanVector, IntervalDayTimeVector, IntervalMonthDayNanoVector, IntervalYearMonthVector,
        TimeMicrosecondVector, TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector,
        TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
        TimestampSecondVector, Vector,
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
        let values = values.ts_millisecond_values;
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
    }

    #[test]
    fn test_concrete_datatype_from_column_datatype() {
        assert_eq!(
            ConcreteDataType::boolean_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Boolean).into()
        );
        assert_eq!(
            ConcreteDataType::int8_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int8).into()
        );
        assert_eq!(
            ConcreteDataType::int16_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int16).into()
        );
        assert_eq!(
            ConcreteDataType::int32_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int32).into()
        );
        assert_eq!(
            ConcreteDataType::int64_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Int64).into()
        );
        assert_eq!(
            ConcreteDataType::uint8_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint8).into()
        );
        assert_eq!(
            ConcreteDataType::uint16_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint16).into()
        );
        assert_eq!(
            ConcreteDataType::uint32_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint32).into()
        );
        assert_eq!(
            ConcreteDataType::uint64_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Uint64).into()
        );
        assert_eq!(
            ConcreteDataType::float32_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Float32).into()
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Float64).into()
        );
        assert_eq!(
            ConcreteDataType::binary_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Binary).into()
        );
        assert_eq!(
            ConcreteDataType::string_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::String).into()
        );
        assert_eq!(
            ConcreteDataType::date_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Date).into()
        );
        assert_eq!(
            ConcreteDataType::datetime_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::Datetime).into()
        );
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            ColumnDataTypeWrapper(ColumnDataType::TimestampMillisecond).into()
        );
        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ColumnDataTypeWrapper(ColumnDataType::TimeMillisecond).into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime),
            ColumnDataTypeWrapper(ColumnDataType::IntervalDayTime).into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth),
            ColumnDataTypeWrapper(ColumnDataType::IntervalYearMonth).into()
        );
        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ColumnDataTypeWrapper(ColumnDataType::IntervalMonthDayNano).into()
        );
    }

    #[test]
    fn test_column_datatype_from_concrete_datatype() {
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Boolean),
            ConcreteDataType::boolean_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int8),
            ConcreteDataType::int8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int16),
            ConcreteDataType::int16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int32),
            ConcreteDataType::int32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Int64),
            ConcreteDataType::int64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint8),
            ConcreteDataType::uint8_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint16),
            ConcreteDataType::uint16_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint32),
            ConcreteDataType::uint32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Uint64),
            ConcreteDataType::uint64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Float32),
            ConcreteDataType::float32_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Float64),
            ConcreteDataType::float64_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Binary),
            ConcreteDataType::binary_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::String),
            ConcreteDataType::string_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Date),
            ConcreteDataType::date_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::Datetime),
            ConcreteDataType::datetime_datatype().try_into().unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::TimestampMillisecond),
            ConcreteDataType::timestamp_millisecond_datatype()
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::IntervalYearMonth),
            ConcreteDataType::interval_datatype(IntervalUnit::YearMonth)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::IntervalDayTime),
            ConcreteDataType::interval_datatype(IntervalUnit::DayTime)
                .try_into()
                .unwrap()
        );
        assert_eq!(
            ColumnDataTypeWrapper(ColumnDataType::IntervalMonthDayNano),
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano)
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
        };

        let vector = Arc::new(TimestampNanosecondVector::from_vec(vec![1, 2, 3]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![1, 2, 3],
            column.values.as_ref().unwrap().ts_nanosecond_values
        );

        let vector = Arc::new(TimestampMillisecondVector::from_vec(vec![4, 5, 6]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![4, 5, 6],
            column.values.as_ref().unwrap().ts_millisecond_values
        );

        let vector = Arc::new(TimestampMicrosecondVector::from_vec(vec![7, 8, 9]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![7, 8, 9],
            column.values.as_ref().unwrap().ts_microsecond_values
        );

        let vector = Arc::new(TimestampSecondVector::from_vec(vec![10, 11, 12]));
        push_vals(&mut column, 3, vector);
        assert_eq!(
            vec![10, 11, 12],
            column.values.as_ref().unwrap().ts_second_values
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
                ts_second_values: vec![1_i64, 2_i64, 3_i64],
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
                ts_millisecond_values: vec![1_i64, 2_i64, 3_i64],
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
                    IntervalMonthDayNano {
                        months: 1,
                        days: 2,
                        nanoseconds: 3,
                    },
                    IntervalMonthDayNano {
                        months: 5,
                        days: 6,
                        nanoseconds: 7,
                    },
                    IntervalMonthDayNano {
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
}
