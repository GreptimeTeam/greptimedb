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

use common_base::BitVec;
use common_time::interval::IntervalUnit;
use common_time::timestamp::TimeUnit;
use common_time::Interval;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::{IntervalType, TimeType, TimestampType};
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use greptime_proto::v1::ddl_request::Expr;
use greptime_proto::v1::greptime_request::Request;
use greptime_proto::v1::query_request::Query;
use greptime_proto::v1::{DdlRequest, IntervalMonthDayNano, QueryRequest};
use snafu::prelude::*;

use crate::error::{self, Result};
use crate::v1::column::Values;
use crate::v1::{Column, ColumnDataType};

#[derive(Debug, PartialEq, Eq)]
pub struct ColumnDataTypeWrapper(ColumnDataType);

impl ColumnDataTypeWrapper {
    pub fn try_new(datatype: i32) -> Result<Self> {
        let datatype = ColumnDataType::from_i32(datatype)
            .context(error::UnknownColumnDataTypeSnafu { datatype })?;
        Ok(Self(datatype))
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
        Request::Inserts(_) | Request::RowInserts(_) => "inserts",
        Request::Query(query_req) => query_request_type(query_req),
        Request::Ddl(ddl_req) => ddl_request_type(ddl_req),
        Request::Delete(_) | Request::RowDelete(_) => "delete",
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
        Some(Expr::FlushTable(_)) => "ddl.flush_table",
        Some(Expr::CompactTable(_)) => "ddl.compact_table",
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::vectors::{
        BooleanVector, IntervalDayTimeVector, IntervalMonthDayNanoVector, IntervalYearMonthVector,
        TimeMicrosecondVector, TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector,
        TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
        TimestampSecondVector, Vector,
    };

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
}
