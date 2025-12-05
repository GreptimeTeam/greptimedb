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

mod bytea;
mod datetime;
mod error;
mod interval;

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{
    Date32Type, Date64Type, Decimal128Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType,
    Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_schema::{DataType, IntervalUnit, TimeUnit};
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime};
use common_decimal::Decimal128;
use common_recordbatch::RecordBatch;
use common_time::time::Time;
use common_time::{Date, IntervalDayTime, IntervalMonthDayNano, IntervalYearMonth, Timestamp};
use datafusion_common::ScalarValue;
use datafusion_expr::LogicalPlan;
use datatypes::arrow::datatypes::DataType as ArrowDataType;
use datatypes::json::JsonStructureSettings;
use datatypes::prelude::{ConcreteDataType, Value};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::types::{IntervalType, TimestampType, jsonb_to_string};
use datatypes::value::StructValue;
use pgwire::api::Type;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::results::{DataRowEncoder, FieldInfo};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::data::DataRow;
use session::context::QueryContextRef;
use session::session_config::PGByteaOutputValue;
use snafu::ResultExt;

use self::bytea::{EscapeOutputBytea, HexOutputBytea};
use self::datetime::{StylingDate, StylingDateTime};
pub use self::error::{PgErrorCode, PgErrorSeverity};
use self::interval::PgInterval;
use crate::SqlPlan;
use crate::error::{self as server_error, DataFusionSnafu, Error, Result};
use crate::postgres::utils::convert_err;

pub(super) fn schema_to_pg(origin: &Schema, field_formats: &Format) -> Result<Vec<FieldInfo>> {
    origin
        .column_schemas()
        .iter()
        .enumerate()
        .map(|(idx, col)| {
            Ok(FieldInfo::new(
                col.name.clone(),
                None,
                None,
                type_gt_to_pg(&col.data_type)?,
                field_formats.format_for(idx),
            ))
        })
        .collect::<Result<Vec<FieldInfo>>>()
}

/// this function will encode greptime's `StructValue` into PostgreSQL jsonb type
///
/// Note that greptimedb has different types of StructValue for storing json data,
/// based on policy defined in `JsonStructureSettings`. But here the `StructValue`
/// should be fully structured.
///
/// there are alternatives like records, arrays, etc. but there are also limitations:
/// records: there is no support for include keys
/// arrays: element in array must be the same type
fn encode_struct(
    _query_ctx: &QueryContextRef,
    struct_value: StructValue,
    builder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    let encoding_setting = JsonStructureSettings::Structured(None);
    let json_value = encoding_setting
        .decode(Value::Struct(struct_value))
        .map_err(|e| PgWireError::ApiError(Box::new(e)))?;

    builder.encode_field(&json_value)
}

fn encode_array(
    query_ctx: &QueryContextRef,
    array: ArrayRef,
    builder: &mut DataRowEncoder,
) -> PgWireResult<()> {
    macro_rules! encode_primitive_array {
        ($array: ident, $data_type: ty, $lower_type: ty, $upper_type: ty) => {{
            let array = $array.iter().collect::<Vec<Option<$data_type>>>();
            if array
                .iter()
                .all(|x| x.is_none_or(|i| i <= <$lower_type>::MAX as $data_type))
            {
                builder.encode_field(
                    &array
                        .into_iter()
                        .map(|x| x.map(|i| i as $lower_type))
                        .collect::<Vec<Option<$lower_type>>>(),
                )
            } else {
                builder.encode_field(
                    &array
                        .into_iter()
                        .map(|x| x.map(|i| i as $upper_type))
                        .collect::<Vec<Option<$upper_type>>>(),
                )
            }
        }};
    }

    match array.data_type() {
        DataType::Boolean => {
            let array = array.as_boolean();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Int8 => {
            let array = array.as_primitive::<Int8Type>();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Int16 => {
            let array = array.as_primitive::<Int16Type>();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Int32 => {
            let array = array.as_primitive::<Int32Type>();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Int64 => {
            let array = array.as_primitive::<Int64Type>();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::UInt8 => {
            let array = array.as_primitive::<UInt8Type>();
            encode_primitive_array!(array, u8, i8, i16)
        }
        DataType::UInt16 => {
            let array = array.as_primitive::<UInt16Type>();
            encode_primitive_array!(array, u16, i16, i32)
        }
        DataType::UInt32 => {
            let array = array.as_primitive::<UInt32Type>();
            encode_primitive_array!(array, u32, i32, i64)
        }
        DataType::UInt64 => {
            let array = array.as_primitive::<UInt64Type>();
            let array = array.iter().collect::<Vec<_>>();
            if array.iter().all(|x| x.is_none_or(|i| i <= i64::MAX as u64)) {
                builder.encode_field(
                    &array
                        .into_iter()
                        .map(|x| x.map(|i| i as i64))
                        .collect::<Vec<Option<i64>>>(),
                )
            } else {
                builder.encode_field(
                    &array
                        .into_iter()
                        .map(|x| x.map(|i| i.to_string()))
                        .collect::<Vec<_>>(),
                )
            }
        }
        DataType::Float32 => {
            let array = array.as_primitive::<Float32Type>();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Float64 => {
            let array = array.as_primitive::<Float64Type>();
            let array = array.iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Binary => {
            let bytea_output = query_ctx.configuration_parameter().postgres_bytea_output();

            let array = array.as_binary::<i32>();
            match *bytea_output {
                PGByteaOutputValue::ESCAPE => {
                    let array = array
                        .iter()
                        .map(|v| v.map(EscapeOutputBytea))
                        .collect::<Vec<_>>();
                    builder.encode_field(&array)
                }
                PGByteaOutputValue::HEX => {
                    let array = array
                        .iter()
                        .map(|v| v.map(HexOutputBytea))
                        .collect::<Vec<_>>();
                    builder.encode_field(&array)
                }
            }
        }
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            let array = array.into_iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::LargeUtf8 => {
            let array = array.as_string::<i64>();
            let array = array.into_iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Utf8View => {
            let array = array.as_string_view();
            let array = array.into_iter().collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        DataType::Date32 | DataType::Date64 => {
            let iter: Box<dyn Iterator<Item = Option<i32>>> =
                if matches!(array.data_type(), DataType::Date32) {
                    let array = array.as_primitive::<Date32Type>();
                    Box::new(array.into_iter())
                } else {
                    let array = array.as_primitive::<Date64Type>();
                    // `Date64` values are milliseconds representation of `Date32` values, according
                    // to its specification. So we convert them to `Date32` values to process the
                    // `Date64` array unified with `Date32` array.
                    Box::new(
                        array
                            .into_iter()
                            .map(|x| x.map(|i| (i / 86_400_000) as i32)),
                    )
                };
            let array = iter
                .into_iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(v) => {
                        if let Some(date) = Date::new(v).to_chrono_date() {
                            let (style, order) =
                                *query_ctx.configuration_parameter().pg_datetime_style();
                            Ok(Some(StylingDate(date, style, order)))
                        } else {
                            Err(convert_err(Error::Internal {
                                err_msg: format!("Failed to convert date to postgres type {v:?}",),
                            }))
                        }
                    }
                })
                .collect::<PgWireResult<Vec<Option<StylingDate>>>>()?;
            builder.encode_field(&array)
        }
        DataType::Timestamp(time_unit, _) => {
            let array = match time_unit {
                TimeUnit::Second => {
                    let array = array.as_primitive::<TimestampSecondType>();
                    array.into_iter().collect::<Vec<_>>()
                }
                TimeUnit::Millisecond => {
                    let array = array.as_primitive::<TimestampMillisecondType>();
                    array.into_iter().collect::<Vec<_>>()
                }
                TimeUnit::Microsecond => {
                    let array = array.as_primitive::<TimestampMicrosecondType>();
                    array.into_iter().collect::<Vec<_>>()
                }
                TimeUnit::Nanosecond => {
                    let array = array.as_primitive::<TimestampNanosecondType>();
                    array.into_iter().collect::<Vec<_>>()
                }
            };
            let time_unit = time_unit.into();
            let array = array
                .into_iter()
                .map(|v| match v {
                    None => Ok(None),
                    Some(v) => {
                        let v = Timestamp::new(v, time_unit);
                        if let Some(datetime) =
                            v.to_chrono_datetime_with_timezone(Some(&query_ctx.timezone()))
                        {
                            let (style, order) =
                                *query_ctx.configuration_parameter().pg_datetime_style();
                            Ok(Some(StylingDateTime(datetime, style, order)))
                        } else {
                            Err(convert_err(Error::Internal {
                                err_msg: format!("Failed to convert date to postgres type {v:?}",),
                            }))
                        }
                    }
                })
                .collect::<PgWireResult<Vec<Option<StylingDateTime>>>>()?;
            builder.encode_field(&array)
        }
        DataType::Time32(time_unit) | DataType::Time64(time_unit) => {
            let iter: Box<dyn Iterator<Item = Option<Time>>> = match time_unit {
                TimeUnit::Second => {
                    let array = array.as_primitive::<Time32SecondType>();
                    Box::new(
                        array
                            .into_iter()
                            .map(|v| v.map(|i| Time::new_second(i as i64))),
                    )
                }
                TimeUnit::Millisecond => {
                    let array = array.as_primitive::<Time32MillisecondType>();
                    Box::new(
                        array
                            .into_iter()
                            .map(|v| v.map(|i| Time::new_millisecond(i as i64))),
                    )
                }
                TimeUnit::Microsecond => {
                    let array = array.as_primitive::<Time64MicrosecondType>();
                    Box::new(array.into_iter().map(|v| v.map(Time::new_microsecond)))
                }
                TimeUnit::Nanosecond => {
                    let array = array.as_primitive::<Time64NanosecondType>();
                    Box::new(array.into_iter().map(|v| v.map(Time::new_nanosecond)))
                }
            };
            let array = iter
                .into_iter()
                .map(|v| v.and_then(|v| v.to_chrono_time()))
                .collect::<Vec<Option<NaiveTime>>>();
            builder.encode_field(&array)
        }
        DataType::Interval(interval_unit) => {
            let array = match interval_unit {
                IntervalUnit::YearMonth => {
                    let array = array.as_primitive::<IntervalYearMonthType>();
                    array
                        .into_iter()
                        .map(|v| v.map(|i| PgInterval::from(IntervalYearMonth::from(i))))
                        .collect::<Vec<_>>()
                }
                IntervalUnit::DayTime => {
                    let array = array.as_primitive::<IntervalDayTimeType>();
                    array
                        .into_iter()
                        .map(|v| v.map(|i| PgInterval::from(IntervalDayTime::from(i))))
                        .collect::<Vec<_>>()
                }
                IntervalUnit::MonthDayNano => {
                    let array = array.as_primitive::<IntervalMonthDayNanoType>();
                    array
                        .into_iter()
                        .map(|v| v.map(|i| PgInterval::from(IntervalMonthDayNano::from(i))))
                        .collect::<Vec<_>>()
                }
            };
            builder.encode_field(&array)
        }
        DataType::Decimal128(precision, scale) => {
            let array = array.as_primitive::<Decimal128Type>();
            let array = array
                .into_iter()
                .map(|v| v.map(|i| Decimal128::new(i, *precision, *scale).to_string()))
                .collect::<Vec<_>>();
            builder.encode_field(&array)
        }
        _ => Err(convert_err(Error::Internal {
            err_msg: format!(
                "cannot write array type {:?} in postgres protocol: unimplemented",
                array.data_type()
            ),
        })),
    }
}

pub(crate) struct RecordBatchRowIterator {
    query_ctx: QueryContextRef,
    pg_schema: Arc<Vec<FieldInfo>>,
    schema: SchemaRef,
    record_batch: arrow::record_batch::RecordBatch,
    i: usize,
}

impl Iterator for RecordBatchRowIterator {
    type Item = PgWireResult<DataRow>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i < self.record_batch.num_rows() {
            let mut encoder = DataRowEncoder::new(self.pg_schema.clone());
            if let Err(e) = self.encode_row(self.i, &mut encoder) {
                return Some(Err(e));
            }
            self.i += 1;
            Some(encoder.finish())
        } else {
            None
        }
    }
}

impl RecordBatchRowIterator {
    pub(crate) fn new(
        query_ctx: QueryContextRef,
        pg_schema: Arc<Vec<FieldInfo>>,
        record_batch: RecordBatch,
    ) -> Self {
        let schema = record_batch.schema.clone();
        let record_batch = record_batch.into_df_record_batch();
        Self {
            query_ctx,
            pg_schema,
            schema,
            record_batch,
            i: 0,
        }
    }

    fn encode_row(&mut self, i: usize, encoder: &mut DataRowEncoder) -> PgWireResult<()> {
        for (j, column) in self.record_batch.columns().iter().enumerate() {
            if column.is_null(i) {
                encoder.encode_field(&None::<&i8>)?;
                continue;
            }

            match column.data_type() {
                DataType::Null => {
                    encoder.encode_field(&None::<&i8>)?;
                }
                DataType::Boolean => {
                    let array = column.as_boolean();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::UInt8 => {
                    let array = column.as_primitive::<UInt8Type>();
                    let value = array.value(i);
                    if value <= i8::MAX as u8 {
                        encoder.encode_field(&(value as i8))?;
                    } else {
                        encoder.encode_field(&(value as i16))?;
                    }
                }
                DataType::UInt16 => {
                    let array = column.as_primitive::<UInt16Type>();
                    let value = array.value(i);
                    if value <= i16::MAX as u16 {
                        encoder.encode_field(&(value as i16))?;
                    } else {
                        encoder.encode_field(&(value as i32))?;
                    }
                }
                DataType::UInt32 => {
                    let array = column.as_primitive::<UInt32Type>();
                    let value = array.value(i);
                    if value <= i32::MAX as u32 {
                        encoder.encode_field(&(value as i32))?;
                    } else {
                        encoder.encode_field(&(value as i64))?;
                    }
                }
                DataType::UInt64 => {
                    let array = column.as_primitive::<UInt64Type>();
                    let value = array.value(i);
                    if value <= i64::MAX as u64 {
                        encoder.encode_field(&(value as i64))?;
                    } else {
                        encoder.encode_field(&value.to_string())?;
                    }
                }
                DataType::Int8 => {
                    let array = column.as_primitive::<Int8Type>();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::Int16 => {
                    let array = column.as_primitive::<Int16Type>();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::Int32 => {
                    let array = column.as_primitive::<Int32Type>();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::Int64 => {
                    let array = column.as_primitive::<Int64Type>();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::Float32 => {
                    let array = column.as_primitive::<Float32Type>();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::Float64 => {
                    let array = column.as_primitive::<Float64Type>();
                    encoder.encode_field(&array.value(i))?;
                }
                DataType::Utf8 => {
                    let array = column.as_string::<i32>();
                    let value = array.value(i);
                    encoder.encode_field(&value)?;
                }
                DataType::Utf8View => {
                    let array = column.as_string_view();
                    let value = array.value(i);
                    encoder.encode_field(&value)?;
                }
                DataType::LargeUtf8 => {
                    let array = column.as_string::<i64>();
                    let value = array.value(i);
                    encoder.encode_field(&value)?;
                }
                DataType::Binary => {
                    let array = column.as_binary::<i32>();
                    let v = array.value(i);
                    encode_bytes(
                        &self.schema.column_schemas()[j],
                        v,
                        encoder,
                        &self.query_ctx,
                    )?;
                }
                DataType::BinaryView => {
                    let array = column.as_binary_view();
                    let v = array.value(i);
                    encode_bytes(
                        &self.schema.column_schemas()[j],
                        v,
                        encoder,
                        &self.query_ctx,
                    )?;
                }
                DataType::LargeBinary => {
                    let array = column.as_binary::<i64>();
                    let v = array.value(i);
                    encode_bytes(
                        &self.schema.column_schemas()[j],
                        v,
                        encoder,
                        &self.query_ctx,
                    )?;
                }
                DataType::Date32 | DataType::Date64 => {
                    let v = if matches!(column.data_type(), DataType::Date32) {
                        let array = column.as_primitive::<Date32Type>();
                        array.value(i)
                    } else {
                        let array = column.as_primitive::<Date64Type>();
                        // `Date64` values are milliseconds representation of `Date32` values,
                        // according to its specification. So we convert the `Date64` value here to
                        // the `Date32` value to process them unified.
                        (array.value(i) / 86_400_000) as i32
                    };
                    let v = Date::new(v);
                    let date = v.to_chrono_date().map(|v| {
                        let (style, order) =
                            *self.query_ctx.configuration_parameter().pg_datetime_style();
                        StylingDate(v, style, order)
                    });
                    encoder.encode_field(&date)?;
                }
                DataType::Timestamp(_, _) => {
                    let v = datatypes::arrow_array::timestamp_array_value(column, i);
                    let datetime = v
                        .to_chrono_datetime_with_timezone(Some(&self.query_ctx.timezone()))
                        .map(|v| {
                            let (style, order) =
                                *self.query_ctx.configuration_parameter().pg_datetime_style();
                            StylingDateTime(v, style, order)
                        });
                    encoder.encode_field(&datetime)?;
                }
                DataType::Interval(interval_unit) => match interval_unit {
                    IntervalUnit::YearMonth => {
                        let array = column.as_primitive::<IntervalYearMonthType>();
                        let v: IntervalYearMonth = array.value(i).into();
                        encoder.encode_field(&PgInterval::from(v))?;
                    }
                    IntervalUnit::DayTime => {
                        let array = column.as_primitive::<IntervalDayTimeType>();
                        let v: IntervalDayTime = array.value(i).into();
                        encoder.encode_field(&PgInterval::from(v))?;
                    }
                    IntervalUnit::MonthDayNano => {
                        let array = column.as_primitive::<IntervalMonthDayNanoType>();
                        let v: IntervalMonthDayNano = array.value(i).into();
                        encoder.encode_field(&PgInterval::from(v))?;
                    }
                },
                DataType::Duration(_) => {
                    let d = datatypes::arrow_array::duration_array_value(column, i);
                    match PgInterval::try_from(d) {
                        Ok(i) => encoder.encode_field(&i)?,
                        Err(e) => {
                            return Err(convert_err(Error::Internal {
                                err_msg: e.to_string(),
                            }));
                        }
                    }
                }
                DataType::List(_) => {
                    let array = column.as_list::<i32>();
                    let items = array.value(i);
                    encode_array(&self.query_ctx, items, encoder)?;
                }
                DataType::Struct(_) => {
                    encode_struct(&self.query_ctx, Default::default(), encoder)?;
                }
                DataType::Time32(_) | DataType::Time64(_) => {
                    let v = datatypes::arrow_array::time_array_value(column, i);
                    encoder.encode_field(&v.to_chrono_time())?;
                }
                DataType::Decimal128(precision, scale) => {
                    let array = column.as_primitive::<Decimal128Type>();
                    let v = Decimal128::new(array.value(i), *precision, *scale);
                    encoder.encode_field(&v.to_string())?;
                }
                _ => {
                    return Err(convert_err(Error::Internal {
                        err_msg: format!(
                            "cannot convert datatype {} to postgres",
                            column.data_type()
                        ),
                    }));
                }
            }
        }
        Ok(())
    }
}

fn encode_bytes(
    schema: &ColumnSchema,
    v: &[u8],
    encoder: &mut DataRowEncoder,
    query_ctx: &QueryContextRef,
) -> PgWireResult<()> {
    if let ConcreteDataType::Json(_) = &schema.data_type {
        let s = jsonb_to_string(v).map_err(convert_err)?;
        encoder.encode_field(&s)
    } else {
        let bytea_output = query_ctx.configuration_parameter().postgres_bytea_output();
        match *bytea_output {
            PGByteaOutputValue::ESCAPE => encoder.encode_field(&EscapeOutputBytea(v)),
            PGByteaOutputValue::HEX => encoder.encode_field(&HexOutputBytea(v)),
        }
    }
}

pub(super) fn type_gt_to_pg(origin: &ConcreteDataType) -> Result<Type> {
    match origin {
        &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
        &ConcreteDataType::Boolean(_) => Ok(Type::BOOL),
        &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => Ok(Type::CHAR),
        &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT2),
        &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT4),
        &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => Ok(Type::INT8),
        &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4),
        &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8),
        &ConcreteDataType::Binary(_) | &ConcreteDataType::Vector(_) => Ok(Type::BYTEA),
        &ConcreteDataType::String(_) => Ok(Type::VARCHAR),
        &ConcreteDataType::Date(_) => Ok(Type::DATE),
        &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP),
        &ConcreteDataType::Time(_) => Ok(Type::TIME),
        &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC),
        &ConcreteDataType::Json(_) => Ok(Type::JSON),
        ConcreteDataType::List(list) => match list.item_type() {
            &ConcreteDataType::Null(_) => Ok(Type::UNKNOWN),
            &ConcreteDataType::Boolean(_) => Ok(Type::BOOL_ARRAY),
            &ConcreteDataType::Int8(_) | &ConcreteDataType::UInt8(_) => Ok(Type::CHAR_ARRAY),
            &ConcreteDataType::Int16(_) | &ConcreteDataType::UInt16(_) => Ok(Type::INT2_ARRAY),
            &ConcreteDataType::Int32(_) | &ConcreteDataType::UInt32(_) => Ok(Type::INT4_ARRAY),
            &ConcreteDataType::Int64(_) | &ConcreteDataType::UInt64(_) => Ok(Type::INT8_ARRAY),
            &ConcreteDataType::Float32(_) => Ok(Type::FLOAT4_ARRAY),
            &ConcreteDataType::Float64(_) => Ok(Type::FLOAT8_ARRAY),
            &ConcreteDataType::Binary(_) => Ok(Type::BYTEA_ARRAY),
            &ConcreteDataType::String(_) => Ok(Type::VARCHAR_ARRAY),
            &ConcreteDataType::Date(_) => Ok(Type::DATE_ARRAY),
            &ConcreteDataType::Timestamp(_) => Ok(Type::TIMESTAMP_ARRAY),
            &ConcreteDataType::Time(_) => Ok(Type::TIME_ARRAY),
            &ConcreteDataType::Interval(_) => Ok(Type::INTERVAL_ARRAY),
            &ConcreteDataType::Decimal128(_) => Ok(Type::NUMERIC_ARRAY),
            &ConcreteDataType::Json(_) => Ok(Type::JSON_ARRAY),
            &ConcreteDataType::Duration(_) => Ok(Type::INTERVAL_ARRAY),
            &ConcreteDataType::Struct(_) => Ok(Type::JSON_ARRAY),
            &ConcreteDataType::Dictionary(_)
            | &ConcreteDataType::Vector(_)
            | &ConcreteDataType::List(_) => server_error::UnsupportedDataTypeSnafu {
                data_type: origin,
                reason: "not implemented",
            }
            .fail(),
        },
        &ConcreteDataType::Dictionary(_) => server_error::UnsupportedDataTypeSnafu {
            data_type: origin,
            reason: "not implemented",
        }
        .fail(),
        &ConcreteDataType::Duration(_) => Ok(Type::INTERVAL),
        &ConcreteDataType::Struct(_) => Ok(Type::JSON),
    }
}

#[allow(dead_code)]
pub(super) fn type_pg_to_gt(origin: &Type) -> Result<ConcreteDataType> {
    // Note that we only support a small amount of pg data types
    match origin {
        &Type::BOOL => Ok(ConcreteDataType::boolean_datatype()),
        &Type::CHAR => Ok(ConcreteDataType::int8_datatype()),
        &Type::INT2 => Ok(ConcreteDataType::int16_datatype()),
        &Type::INT4 => Ok(ConcreteDataType::int32_datatype()),
        &Type::INT8 => Ok(ConcreteDataType::int64_datatype()),
        &Type::VARCHAR | &Type::TEXT => Ok(ConcreteDataType::string_datatype()),
        &Type::TIMESTAMP | &Type::TIMESTAMPTZ => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Millisecond,
        )),
        &Type::DATE => Ok(ConcreteDataType::date_datatype()),
        &Type::TIME => Ok(ConcreteDataType::timestamp_datatype(
            common_time::timestamp::TimeUnit::Microsecond,
        )),
        &Type::CHAR_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int8_datatype(),
        ))),
        &Type::INT2_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int16_datatype(),
        ))),
        &Type::INT4_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int32_datatype(),
        ))),
        &Type::INT8_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::int64_datatype(),
        ))),
        &Type::VARCHAR_ARRAY => Ok(ConcreteDataType::list_datatype(Arc::new(
            ConcreteDataType::string_datatype(),
        ))),
        _ => server_error::InternalSnafu {
            err_msg: format!("unimplemented datatype {origin:?}"),
        }
        .fail(),
    }
}

pub(super) fn parameter_to_string(portal: &Portal<SqlPlan>, idx: usize) -> PgWireResult<String> {
    // the index is managed from portal's parameters count so it's safe to
    // unwrap here.
    let param_type = portal
        .statement
        .parameter_types
        .get(idx)
        .unwrap()
        .as_ref()
        .unwrap_or(&Type::UNKNOWN);
    match param_type {
        &Type::VARCHAR | &Type::TEXT => Ok(format!(
            "'{}'",
            portal
                .parameter::<String>(idx, param_type)?
                .as_deref()
                .unwrap_or("")
        )),
        &Type::BOOL => Ok(portal
            .parameter::<bool>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT4 => Ok(portal
            .parameter::<i32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INT8 => Ok(portal
            .parameter::<i64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT4 => Ok(portal
            .parameter::<f32>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::FLOAT8 => Ok(portal
            .parameter::<f64>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::DATE => Ok(portal
            .parameter::<NaiveDate>(idx, param_type)?
            .map(|v| v.format("%Y-%m-%d").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::TIMESTAMP => Ok(portal
            .parameter::<NaiveDateTime>(idx, param_type)?
            .map(|v| v.format("%Y-%m-%d %H:%M:%S%.6f").to_string())
            .unwrap_or_else(|| "".to_owned())),
        &Type::INTERVAL => Ok(portal
            .parameter::<PgInterval>(idx, param_type)?
            .map(|v| v.to_string())
            .unwrap_or_else(|| "".to_owned())),
        _ => Err(invalid_parameter_error(
            "unsupported_parameter_type",
            Some(param_type.to_string()),
        )),
    }
}

pub(super) fn invalid_parameter_error(msg: &str, detail: Option<String>) -> PgWireError {
    let mut error_info = PgErrorCode::Ec22023.to_err_info(msg.to_string());
    error_info.detail = detail;
    PgWireError::UserError(Box::new(error_info))
}

fn to_timestamp_scalar_value<T>(
    data: Option<T>,
    unit: &TimestampType,
    ctype: &ConcreteDataType,
) -> PgWireResult<ScalarValue>
where
    T: Into<i64>,
{
    if let Some(n) = data {
        Value::Timestamp(unit.create_timestamp(n.into()))
            .try_to_scalar_value(ctype)
            .map_err(convert_err)
    } else {
        Ok(ScalarValue::Null)
    }
}

pub(super) fn parameters_to_scalar_values(
    plan: &LogicalPlan,
    portal: &Portal<SqlPlan>,
) -> PgWireResult<Vec<ScalarValue>> {
    let param_count = portal.parameter_len();
    let mut results = Vec::with_capacity(param_count);

    let client_param_types = &portal.statement.parameter_types;
    let server_param_types = plan
        .get_parameter_types()
        .context(DataFusionSnafu)
        .map_err(convert_err)?
        .into_iter()
        .map(|(k, v)| (k, v.map(|v| ConcreteDataType::from_arrow_type(&v))))
        .collect::<HashMap<_, _>>();

    for idx in 0..param_count {
        let server_type = server_param_types
            .get(&format!("${}", idx + 1))
            .and_then(|t| t.as_ref());

        let client_type = if let Some(Some(client_given_type)) = client_param_types.get(idx) {
            client_given_type.clone()
        } else if let Some(server_provided_type) = &server_type {
            type_gt_to_pg(server_provided_type).map_err(convert_err)?
        } else {
            return Err(invalid_parameter_error(
                "unknown_parameter_type",
                Some(format!(
                    "Cannot get parameter type information for parameter {}",
                    idx
                )),
            ));
        };

        let value = match &client_type {
            &Type::VARCHAR | &Type::TEXT => {
                let data = portal.parameter::<String>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::String(t) => {
                            if t.is_large() {
                                ScalarValue::LargeUtf8(data)
                            } else {
                                ScalarValue::Utf8(data)
                            }
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Utf8(data)
                }
            }
            &Type::BOOL => {
                let data = portal.parameter::<bool>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Boolean(_) => ScalarValue::Boolean(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Boolean(data)
                }
            }
            &Type::INT2 => {
                let data = portal.parameter::<i16>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Timestamp(unit) => {
                            to_timestamp_scalar_value(data, unit, server_type)?
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Int16(data)
                }
            }
            &Type::INT4 => {
                let data = portal.parameter::<i32>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Timestamp(unit) => {
                            to_timestamp_scalar_value(data, unit, server_type)?
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Int32(data)
                }
            }
            &Type::INT8 => {
                let data = portal.parameter::<i64>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Timestamp(unit) => {
                            to_timestamp_scalar_value(data, unit, server_type)?
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Int64(data)
                }
            }
            &Type::FLOAT4 => {
                let data = portal.parameter::<f32>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Float32(_) => ScalarValue::Float32(data),
                        ConcreteDataType::Float64(_) => {
                            ScalarValue::Float64(data.map(|n| n as f64))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Float32(data)
                }
            }
            &Type::FLOAT8 => {
                let data = portal.parameter::<f64>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Int8(_) => ScalarValue::Int8(data.map(|n| n as i8)),
                        ConcreteDataType::Int16(_) => ScalarValue::Int16(data.map(|n| n as i16)),
                        ConcreteDataType::Int32(_) => ScalarValue::Int32(data.map(|n| n as i32)),
                        ConcreteDataType::Int64(_) => ScalarValue::Int64(data.map(|n| n as i64)),
                        ConcreteDataType::UInt8(_) => ScalarValue::UInt8(data.map(|n| n as u8)),
                        ConcreteDataType::UInt16(_) => ScalarValue::UInt16(data.map(|n| n as u16)),
                        ConcreteDataType::UInt32(_) => ScalarValue::UInt32(data.map(|n| n as u32)),
                        ConcreteDataType::UInt64(_) => ScalarValue::UInt64(data.map(|n| n as u64)),
                        ConcreteDataType::Float32(_) => {
                            ScalarValue::Float32(data.map(|n| n as f32))
                        }
                        ConcreteDataType::Float64(_) => ScalarValue::Float64(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Float64(data)
                }
            }
            &Type::TIMESTAMP => {
                let data = portal.parameter::<NaiveDateTime>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Timestamp(unit) => match *unit {
                            TimestampType::Second(_) => ScalarValue::TimestampSecond(
                                data.map(|ts| ts.and_utc().timestamp()),
                                None,
                            ),
                            TimestampType::Millisecond(_) => ScalarValue::TimestampMillisecond(
                                data.map(|ts| ts.and_utc().timestamp_millis()),
                                None,
                            ),
                            TimestampType::Microsecond(_) => ScalarValue::TimestampMicrosecond(
                                data.map(|ts| ts.and_utc().timestamp_micros()),
                                None,
                            ),
                            TimestampType::Nanosecond(_) => ScalarValue::TimestampNanosecond(
                                data.and_then(|ts| ts.and_utc().timestamp_nanos_opt()),
                                None,
                            ),
                        },
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::TimestampMillisecond(
                        data.map(|ts| ts.and_utc().timestamp_millis()),
                        None,
                    )
                }
            }
            &Type::TIMESTAMPTZ => {
                let data = portal.parameter::<DateTime<FixedOffset>>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Timestamp(unit) => match *unit {
                            TimestampType::Second(_) => {
                                ScalarValue::TimestampSecond(data.map(|ts| ts.timestamp()), None)
                            }
                            TimestampType::Millisecond(_) => ScalarValue::TimestampMillisecond(
                                data.map(|ts| ts.timestamp_millis()),
                                None,
                            ),
                            TimestampType::Microsecond(_) => ScalarValue::TimestampMicrosecond(
                                data.map(|ts| ts.timestamp_micros()),
                                None,
                            ),
                            TimestampType::Nanosecond(_) => ScalarValue::TimestampNanosecond(
                                data.and_then(|ts| ts.timestamp_nanos_opt()),
                                None,
                            ),
                        },
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::TimestampMillisecond(data.map(|ts| ts.timestamp_millis()), None)
                }
            }
            &Type::DATE => {
                let data = portal.parameter::<NaiveDate>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Date(_) => ScalarValue::Date32(
                            data.map(|d| (d - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32),
                        ),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Date32(
                        data.map(|d| (d - DateTime::UNIX_EPOCH.date_naive()).num_days() as i32),
                    )
                }
            }
            &Type::INTERVAL => {
                let data = portal.parameter::<PgInterval>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Interval(IntervalType::YearMonth(_)) => {
                            ScalarValue::IntervalYearMonth(
                                data.map(|i| {
                                    if i.days != 0 || i.microseconds != 0 {
                                        Err(invalid_parameter_error(
                                            "invalid_parameter_type",
                                            Some(format!(
                                                "Expected: {}, found: {}",
                                                server_type, client_type
                                            )),
                                        ))
                                    } else {
                                        Ok(IntervalYearMonth::new(i.months).to_i32())
                                    }
                                })
                                .transpose()?,
                            )
                        }
                        ConcreteDataType::Interval(IntervalType::DayTime(_)) => {
                            ScalarValue::IntervalDayTime(
                                data.map(|i| {
                                    if i.months != 0 || i.microseconds % 1000 != 0 {
                                        Err(invalid_parameter_error(
                                            "invalid_parameter_type",
                                            Some(format!(
                                                "Expected: {}, found: {}",
                                                server_type, client_type
                                            )),
                                        ))
                                    } else {
                                        Ok(IntervalDayTime::new(
                                            i.days,
                                            (i.microseconds / 1000) as i32,
                                        )
                                        .into())
                                    }
                                })
                                .transpose()?,
                            )
                        }
                        ConcreteDataType::Interval(IntervalType::MonthDayNano(_)) => {
                            ScalarValue::IntervalMonthDayNano(
                                data.map(|i| IntervalMonthDayNano::from(i).into()),
                            )
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::IntervalMonthDayNano(
                        data.map(|i| IntervalMonthDayNano::from(i).into()),
                    )
                }
            }
            &Type::BYTEA => {
                let data = portal.parameter::<Vec<u8>>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::String(t) => {
                            let s = data.map(|d| String::from_utf8_lossy(&d).to_string());
                            if t.is_large() {
                                ScalarValue::LargeUtf8(s)
                            } else {
                                ScalarValue::Utf8(s)
                            }
                        }
                        ConcreteDataType::Binary(_) => ScalarValue::Binary(data),
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Binary(data)
                }
            }
            &Type::JSONB => {
                let data = portal.parameter::<serde_json::Value>(idx, &client_type)?;
                if let Some(server_type) = &server_type {
                    match server_type {
                        ConcreteDataType::Binary(_) => {
                            ScalarValue::Binary(data.map(|d| d.to_string().into_bytes()))
                        }
                        _ => {
                            return Err(invalid_parameter_error(
                                "invalid_parameter_type",
                                Some(format!("Expected: {}, found: {}", server_type, client_type)),
                            ));
                        }
                    }
                } else {
                    ScalarValue::Binary(data.map(|d| d.to_string().into_bytes()))
                }
            }
            &Type::INT2_ARRAY => {
                let data = portal.parameter::<Vec<i16>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int16, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::INT4_ARRAY => {
                let data = portal.parameter::<Vec<i32>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int32, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::INT8_ARRAY => {
                let data = portal.parameter::<Vec<i64>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Int64, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::VARCHAR_ARRAY => {
                let data = portal.parameter::<Vec<String>>(idx, &client_type)?;
                if let Some(data) = data {
                    let values = data.into_iter().map(|i| i.into()).collect::<Vec<_>>();
                    ScalarValue::List(ScalarValue::new_list(&values, &ArrowDataType::Utf8, true))
                } else {
                    ScalarValue::Null
                }
            }
            &Type::TIMESTAMP_ARRAY => {
                let data = portal.parameter::<Vec<NaiveDateTime>>(idx, &client_type)?;
                dbg!(&server_type);
                if let Some(data) = data {
                    if let Some(ConcreteDataType::List(list_type)) = &server_type {
                        match list_type.item_type() {
                            ConcreteDataType::Timestamp(unit) => match *unit {
                                TimestampType::Second(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampSecond(
                                                Some(ts.and_utc().timestamp()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Second, None),
                                        true,
                                    ))
                                }
                                TimestampType::Millisecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMillisecond(
                                                Some(ts.and_utc().timestamp_millis()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Microsecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMicrosecond(
                                                Some(ts.and_utc().timestamp_micros()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Nanosecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .filter_map(|ts| {
                                            ts.and_utc().timestamp_nanos_opt().map(|nanos| {
                                                ScalarValue::TimestampNanosecond(Some(nanos), None)
                                            })
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                        true,
                                    ))
                                }
                            },
                            _ => {
                                return Err(invalid_parameter_error(
                                    "invalid_parameter_type",
                                    Some(format!(
                                        "Expected: {}, found: {}",
                                        list_type.item_type(),
                                        client_type
                                    )),
                                ));
                            }
                        }
                    } else {
                        // Default to millisecond when no server type is specified
                        let values = data
                            .into_iter()
                            .map(|ts| {
                                ScalarValue::TimestampMillisecond(
                                    Some(ts.and_utc().timestamp_millis()),
                                    None,
                                )
                            })
                            .collect::<Vec<_>>();
                        ScalarValue::List(ScalarValue::new_list(
                            &values,
                            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        ))
                    }
                } else {
                    ScalarValue::Null
                }
            }
            &Type::TIMESTAMPTZ_ARRAY => {
                let data = portal.parameter::<Vec<DateTime<FixedOffset>>>(idx, &client_type)?;
                if let Some(data) = data {
                    if let Some(ConcreteDataType::List(list_type)) = &server_type {
                        match list_type.item_type() {
                            ConcreteDataType::Timestamp(unit) => match *unit {
                                TimestampType::Second(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampSecond(Some(ts.timestamp()), None)
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Second, None),
                                        true,
                                    ))
                                }
                                TimestampType::Millisecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMillisecond(
                                                Some(ts.timestamp_millis()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Microsecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .map(|ts| {
                                            ScalarValue::TimestampMicrosecond(
                                                Some(ts.timestamp_micros()),
                                                None,
                                            )
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                                        true,
                                    ))
                                }
                                TimestampType::Nanosecond(_) => {
                                    let values = data
                                        .into_iter()
                                        .filter_map(|ts| {
                                            ts.timestamp_nanos_opt().map(|nanos| {
                                                ScalarValue::TimestampNanosecond(Some(nanos), None)
                                            })
                                        })
                                        .collect::<Vec<_>>();
                                    ScalarValue::List(ScalarValue::new_list(
                                        &values,
                                        &ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
                                        true,
                                    ))
                                }
                            },
                            _ => {
                                return Err(invalid_parameter_error(
                                    "invalid_parameter_type",
                                    Some(format!(
                                        "Expected: {}, found: {}",
                                        list_type.item_type(),
                                        client_type
                                    )),
                                ));
                            }
                        }
                    } else {
                        // Default to millisecond when no server type is specified
                        let values = data
                            .into_iter()
                            .map(|ts| {
                                ScalarValue::TimestampMillisecond(Some(ts.timestamp_millis()), None)
                            })
                            .collect::<Vec<_>>();
                        ScalarValue::List(ScalarValue::new_list(
                            &values,
                            &ArrowDataType::Timestamp(TimeUnit::Millisecond, None),
                            true,
                        ))
                    }
                } else {
                    ScalarValue::Null
                }
            }
            _ => Err(invalid_parameter_error(
                "unsupported_parameter_value",
                Some(format!("Found type: {}", client_type)),
            ))?,
        };

        results.push(value);
    }

    Ok(results)
}

pub(super) fn param_types_to_pg_types(
    param_types: &HashMap<String, Option<ConcreteDataType>>,
) -> Result<Vec<Type>> {
    let param_count = param_types.len();
    let mut types = Vec::with_capacity(param_count);
    for i in 0..param_count {
        if let Some(Some(param_type)) = param_types.get(&format!("${}", i + 1)) {
            let pg_type = type_gt_to_pg(param_type)?;
            types.push(pg_type);
        } else {
            types.push(Type::UNKNOWN);
        }
    }
    Ok(types)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{
        Float64Builder, Int64Builder, ListBuilder, StringBuilder, TimestampSecondBuilder,
    };
    use arrow_schema::Field;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{
        BinaryVector, BooleanVector, DateVector, Float32Vector, Float64Vector, Int8Vector,
        Int16Vector, Int32Vector, Int64Vector, IntervalDayTimeVector, IntervalMonthDayNanoVector,
        IntervalYearMonthVector, ListVector, NullVector, StringVector, TimeSecondVector,
        TimestampSecondVector, UInt8Vector, UInt16Vector, UInt32Vector, UInt64Vector, VectorRef,
    };
    use pgwire::api::Type;
    use pgwire::api::results::{FieldFormat, FieldInfo};
    use session::context::QueryContextBuilder;

    use super::*;

    #[test]
    fn test_schema_convert() {
        let column_schemas = vec![
            ColumnSchema::new("nulls", ConcreteDataType::null_datatype(), true),
            ColumnSchema::new("bools", ConcreteDataType::boolean_datatype(), true),
            ColumnSchema::new("int8s", ConcreteDataType::int8_datatype(), true),
            ColumnSchema::new("int16s", ConcreteDataType::int16_datatype(), true),
            ColumnSchema::new("int32s", ConcreteDataType::int32_datatype(), true),
            ColumnSchema::new("int64s", ConcreteDataType::int64_datatype(), true),
            ColumnSchema::new("uint8s", ConcreteDataType::uint8_datatype(), true),
            ColumnSchema::new("uint16s", ConcreteDataType::uint16_datatype(), true),
            ColumnSchema::new("uint32s", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new("uint64s", ConcreteDataType::uint64_datatype(), true),
            ColumnSchema::new("float32s", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new("float64s", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("binaries", ConcreteDataType::binary_datatype(), true),
            ColumnSchema::new("strings", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new(
                "timestamps",
                ConcreteDataType::timestamp_millisecond_datatype(),
                true,
            ),
            ColumnSchema::new("dates", ConcreteDataType::date_datatype(), true),
            ColumnSchema::new("times", ConcreteDataType::time_second_datatype(), true),
            ColumnSchema::new(
                "intervals",
                ConcreteDataType::interval_month_day_nano_datatype(),
                true,
            ),
        ];
        let pg_field_info = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "binaries".into(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "strings".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new("dates".into(), None, None, Type::DATE, FieldFormat::Text),
            FieldInfo::new("times".into(), None, None, Type::TIME, FieldFormat::Text),
            FieldInfo::new(
                "intervals".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
        ];
        let schema = Schema::new(column_schemas);
        let fs = schema_to_pg(&schema, &Format::UnifiedText).unwrap();
        assert_eq!(fs, pg_field_info);
    }

    #[test]
    fn test_encode_text_format_data() {
        let schema = vec![
            FieldInfo::new("nulls".into(), None, None, Type::UNKNOWN, FieldFormat::Text),
            FieldInfo::new("bools".into(), None, None, Type::BOOL, FieldFormat::Text),
            FieldInfo::new("uint8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("uint16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("uint32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("uint64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new("int8s".into(), None, None, Type::CHAR, FieldFormat::Text),
            FieldInfo::new("int16s".into(), None, None, Type::INT2, FieldFormat::Text),
            FieldInfo::new("int32s".into(), None, None, Type::INT4, FieldFormat::Text),
            FieldInfo::new("int64s".into(), None, None, Type::INT8, FieldFormat::Text),
            FieldInfo::new(
                "float32s".into(),
                None,
                None,
                Type::FLOAT4,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float64s".into(),
                None,
                None,
                Type::FLOAT8,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "strings".into(),
                None,
                None,
                Type::VARCHAR,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "binaries".into(),
                None,
                None,
                Type::BYTEA,
                FieldFormat::Text,
            ),
            FieldInfo::new("dates".into(), None, None, Type::DATE, FieldFormat::Text),
            FieldInfo::new("times".into(), None, None, Type::TIME, FieldFormat::Text),
            FieldInfo::new(
                "timestamps".into(),
                None,
                None,
                Type::TIMESTAMP,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_year_month".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_day_time".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "interval_month_day_nano".into(),
                None,
                None,
                Type::INTERVAL,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "int_list".into(),
                None,
                None,
                Type::INT8_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "float_list".into(),
                None,
                None,
                Type::FLOAT8_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "string_list".into(),
                None,
                None,
                Type::VARCHAR_ARRAY,
                FieldFormat::Text,
            ),
            FieldInfo::new(
                "timestamp_list".into(),
                None,
                None,
                Type::TIMESTAMP_ARRAY,
                FieldFormat::Text,
            ),
        ];

        let arrow_schema = arrow_schema::Schema::new(vec![
            Field::new("x", DataType::Null, true),
            Field::new("x", DataType::Boolean, true),
            Field::new("x", DataType::UInt8, true),
            Field::new("x", DataType::UInt16, true),
            Field::new("x", DataType::UInt32, true),
            Field::new("x", DataType::UInt64, true),
            Field::new("x", DataType::Int8, true),
            Field::new("x", DataType::Int16, true),
            Field::new("x", DataType::Int32, true),
            Field::new("x", DataType::Int64, true),
            Field::new("x", DataType::Float32, true),
            Field::new("x", DataType::Float64, true),
            Field::new("x", DataType::Utf8, true),
            Field::new("x", DataType::Binary, true),
            Field::new("x", DataType::Date32, true),
            Field::new("x", DataType::Time32(TimeUnit::Second), true),
            Field::new("x", DataType::Timestamp(TimeUnit::Second, None), true),
            Field::new("x", DataType::Interval(IntervalUnit::YearMonth), true),
            Field::new("x", DataType::Interval(IntervalUnit::DayTime), true),
            Field::new("x", DataType::Interval(IntervalUnit::MonthDayNano), true),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                true,
            ),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "x",
                DataType::List(Arc::new(Field::new(
                    "item",
                    DataType::Timestamp(TimeUnit::Second, None),
                    true,
                ))),
                true,
            ),
        ]);

        let mut builder = ListBuilder::new(Int64Builder::new());
        builder.append_value([Some(1i64), None, Some(2)]);
        builder.append_null();
        builder.append_value([Some(-1i64), None, Some(-2)]);
        let i64_list_array = builder.finish();

        let mut builder = ListBuilder::new(Float64Builder::new());
        builder.append_value([Some(1.0f64), None, Some(2.0)]);
        builder.append_null();
        builder.append_value([Some(-1.0f64), None, Some(-2.0)]);
        let f64_list_array = builder.finish();

        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value([Some("a"), None, Some("b")]);
        builder.append_null();
        builder.append_value([Some("c"), None, Some("d")]);
        let string_list_array = builder.finish();

        let mut builder = ListBuilder::new(TimestampSecondBuilder::new());
        builder.append_value([Some(1i64), None, Some(2)]);
        builder.append_null();
        builder.append_value([Some(3i64), None, Some(4)]);
        let timestamp_list_array = builder.finish();

        let values = vec![
            Arc::new(NullVector::new(3)) as VectorRef,
            Arc::new(BooleanVector::from(vec![Some(true), Some(false), None])),
            Arc::new(UInt8Vector::from(vec![Some(u8::MAX), Some(u8::MIN), None])),
            Arc::new(UInt16Vector::from(vec![
                Some(u16::MAX),
                Some(u16::MIN),
                None,
            ])),
            Arc::new(UInt32Vector::from(vec![
                Some(u32::MAX),
                Some(u32::MIN),
                None,
            ])),
            Arc::new(UInt64Vector::from(vec![
                Some(u64::MAX),
                Some(u64::MIN),
                None,
            ])),
            Arc::new(Int8Vector::from(vec![Some(i8::MAX), Some(i8::MIN), None])),
            Arc::new(Int16Vector::from(vec![
                Some(i16::MAX),
                Some(i16::MIN),
                None,
            ])),
            Arc::new(Int32Vector::from(vec![
                Some(i32::MAX),
                Some(i32::MIN),
                None,
            ])),
            Arc::new(Int64Vector::from(vec![
                Some(i64::MAX),
                Some(i64::MIN),
                None,
            ])),
            Arc::new(Float32Vector::from(vec![
                None,
                Some(f32::MAX),
                Some(f32::MIN),
            ])),
            Arc::new(Float64Vector::from(vec![
                None,
                Some(f64::MAX),
                Some(f64::MIN),
            ])),
            Arc::new(StringVector::from(vec![
                None,
                Some("hello"),
                Some("greptime"),
            ])),
            Arc::new(BinaryVector::from(vec![
                None,
                Some("hello".as_bytes().to_vec()),
                Some("world".as_bytes().to_vec()),
            ])),
            Arc::new(DateVector::from(vec![Some(1001), None, Some(1)])),
            Arc::new(TimeSecondVector::from(vec![Some(1001), None, Some(1)])),
            Arc::new(TimestampSecondVector::from(vec![
                Some(1000001),
                None,
                Some(1),
            ])),
            Arc::new(IntervalYearMonthVector::from(vec![Some(1), None, Some(2)])),
            Arc::new(IntervalDayTimeVector::from(vec![
                Some(arrow::datatypes::IntervalDayTime::new(1, 1)),
                None,
                Some(arrow::datatypes::IntervalDayTime::new(2, 2)),
            ])),
            Arc::new(IntervalMonthDayNanoVector::from(vec![
                Some(arrow::datatypes::IntervalMonthDayNano::new(1, 1, 10)),
                None,
                Some(arrow::datatypes::IntervalMonthDayNano::new(2, 2, 20)),
            ])),
            Arc::new(ListVector::from(i64_list_array)),
            Arc::new(ListVector::from(f64_list_array)),
            Arc::new(ListVector::from(string_list_array)),
            Arc::new(ListVector::from(timestamp_list_array)),
        ];
        let record_batch =
            RecordBatch::new(Arc::new(arrow_schema.try_into().unwrap()), values).unwrap();

        let query_context = QueryContextBuilder::default()
            .configuration_parameter(Default::default())
            .build()
            .into();
        let schema = Arc::new(schema);

        let rows = RecordBatchRowIterator::new(query_context, schema.clone(), record_batch)
            .filter_map(|x| x.ok())
            .collect::<Vec<_>>();
        assert_eq!(rows.len(), 3);
        for row in rows {
            assert_eq!(row.field_count, schema.len() as i16);
        }
    }

    #[test]
    fn test_invalid_parameter() {
        // test for refactor with PgErrorCode
        let msg = "invalid_parameter_count";
        let error = invalid_parameter_error(msg, None);
        if let PgWireError::UserError(value) = error {
            assert_eq!("ERROR", value.severity);
            assert_eq!("22023", value.code);
            assert_eq!(msg, value.message);
        } else {
            panic!("test_invalid_parameter failed");
        }
    }
}
