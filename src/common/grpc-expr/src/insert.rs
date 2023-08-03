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

use std::collections::HashMap;
use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::column::Values;
use api::v1::{
    AddColumns, Column, ColumnDataType, CreateTableExpr, InsertRequest as GrpcInsertRequest,
};
use common_base::BitVec;
use common_time::time::Time;
use common_time::timestamp::Timestamp;
use common_time::{Date, DateTime, Interval};
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::{ValueRef, VectorRef};
use datatypes::scalars::ScalarVector;
use datatypes::schema::SchemaRef;
use datatypes::types::{
    Int16Type, Int8Type, IntervalType, TimeType, TimestampType, UInt16Type, UInt8Type,
};
use datatypes::value::Value;
use datatypes::vectors::{
    BinaryVector, BooleanVector, DateTimeVector, DateVector, Float32Vector, Float64Vector,
    Int32Vector, Int64Vector, IntervalDayTimeVector, IntervalMonthDayNanoVector,
    IntervalYearMonthVector, PrimitiveVector, StringVector, TimeMicrosecondVector,
    TimeMillisecondVector, TimeNanosecondVector, TimeSecondVector, TimestampMicrosecondVector,
    TimestampMillisecondVector, TimestampNanosecondVector, TimestampSecondVector, UInt32Vector,
    UInt64Vector,
};
use snafu::{ensure, OptionExt, ResultExt};
use table::metadata::TableId;
use table::requests::InsertRequest;

use crate::error::{
    ColumnAlreadyExistsSnafu, ColumnDataTypeSnafu, CreateVectorSnafu, InvalidColumnProtoSnafu,
    Result, UnexpectedValuesLengthSnafu,
};
use crate::{build_create_table_expr, extract_new_columns, ColumnExpr};

pub fn find_new_columns(schema: &SchemaRef, columns: &[Column]) -> Result<Option<AddColumns>> {
    let column_exprs = ColumnExpr::from_columns(columns);
    extract_new_columns(schema, column_exprs)
}

pub fn column_to_vector(column: &Column, rows: u32) -> Result<VectorRef> {
    let wrapper = ColumnDataTypeWrapper::try_new(column.datatype).context(ColumnDataTypeSnafu)?;
    let column_datatype = wrapper.datatype();

    let rows = rows as usize;
    let mut vector = ConcreteDataType::from(wrapper).create_mutable_vector(rows);

    if let Some(values) = &column.values {
        let values = collect_column_values(column_datatype, values);
        let mut values_iter = values.into_iter();

        let null_mask = BitVec::from_slice(&column.null_mask);
        let mut nulls_iter = null_mask.iter().by_vals().fuse();

        for i in 0..rows {
            if let Some(true) = nulls_iter.next() {
                vector.push_null();
            } else {
                let value_ref = values_iter
                    .next()
                    .with_context(|| InvalidColumnProtoSnafu {
                        err_msg: format!(
                            "value not found at position {} of column {}",
                            i, &column.column_name
                        ),
                    })?;
                vector
                    .try_push_value_ref(value_ref)
                    .context(CreateVectorSnafu)?;
            }
        }
    } else {
        (0..rows).for_each(|_| vector.push_null());
    }
    Ok(vector.to_vector())
}

fn collect_column_values(column_datatype: ColumnDataType, values: &Values) -> Vec<ValueRef> {
    macro_rules! collect_values {
        ($value: expr, $mapper: expr) => {
            $value.iter().map($mapper).collect::<Vec<ValueRef>>()
        };
    }

    match column_datatype {
        ColumnDataType::Boolean => collect_values!(values.bool_values, |v| ValueRef::from(*v)),
        ColumnDataType::Int8 => collect_values!(values.i8_values, |v| ValueRef::from(*v as i8)),
        ColumnDataType::Int16 => {
            collect_values!(values.i16_values, |v| ValueRef::from(*v as i16))
        }
        ColumnDataType::Int32 => {
            collect_values!(values.i32_values, |v| ValueRef::from(*v))
        }
        ColumnDataType::Int64 => {
            collect_values!(values.i64_values, |v| ValueRef::from(*v))
        }
        ColumnDataType::Uint8 => {
            collect_values!(values.u8_values, |v| ValueRef::from(*v as u8))
        }
        ColumnDataType::Uint16 => {
            collect_values!(values.u16_values, |v| ValueRef::from(*v as u16))
        }
        ColumnDataType::Uint32 => {
            collect_values!(values.u32_values, |v| ValueRef::from(*v))
        }
        ColumnDataType::Uint64 => {
            collect_values!(values.u64_values, |v| ValueRef::from(*v))
        }
        ColumnDataType::Float32 => collect_values!(values.f32_values, |v| ValueRef::from(*v)),
        ColumnDataType::Float64 => collect_values!(values.f64_values, |v| ValueRef::from(*v)),
        ColumnDataType::Binary => {
            collect_values!(values.binary_values, |v| ValueRef::from(v.as_slice()))
        }
        ColumnDataType::String => {
            collect_values!(values.string_values, |v| ValueRef::from(v.as_str()))
        }
        ColumnDataType::Date => {
            collect_values!(values.date_values, |v| ValueRef::Date(Date::new(*v)))
        }
        ColumnDataType::Datetime => {
            collect_values!(values.datetime_values, |v| ValueRef::DateTime(
                DateTime::new(*v)
            ))
        }
        ColumnDataType::TimestampSecond => {
            collect_values!(values.ts_second_values, |v| ValueRef::Timestamp(
                Timestamp::new_second(*v)
            ))
        }
        ColumnDataType::TimestampMillisecond => {
            collect_values!(values.ts_millisecond_values, |v| ValueRef::Timestamp(
                Timestamp::new_millisecond(*v)
            ))
        }
        ColumnDataType::TimestampMicrosecond => {
            collect_values!(values.ts_millisecond_values, |v| ValueRef::Timestamp(
                Timestamp::new_microsecond(*v)
            ))
        }
        ColumnDataType::TimestampNanosecond => {
            collect_values!(values.ts_millisecond_values, |v| ValueRef::Timestamp(
                Timestamp::new_nanosecond(*v)
            ))
        }
        ColumnDataType::TimeSecond => {
            collect_values!(values.time_second_values, |v| ValueRef::Time(
                Time::new_second(*v)
            ))
        }
        ColumnDataType::TimeMillisecond => {
            collect_values!(values.time_millisecond_values, |v| ValueRef::Time(
                Time::new_millisecond(*v)
            ))
        }
        ColumnDataType::TimeMicrosecond => {
            collect_values!(values.time_millisecond_values, |v| ValueRef::Time(
                Time::new_microsecond(*v)
            ))
        }
        ColumnDataType::TimeNanosecond => {
            collect_values!(values.time_millisecond_values, |v| ValueRef::Time(
                Time::new_nanosecond(*v)
            ))
        }
        ColumnDataType::IntervalYearMonth => {
            collect_values!(values.interval_year_month_values, |v| {
                ValueRef::Interval(Interval::from_i32(*v))
            })
        }
        ColumnDataType::IntervalDayTime => {
            collect_values!(values.interval_day_time_values, |v| {
                ValueRef::Interval(Interval::from_i64(*v))
            })
        }
        ColumnDataType::IntervalMonthDayNano => {
            collect_values!(values.interval_month_day_nano_values, |v| {
                ValueRef::Interval(Interval::from_month_day_nano(
                    v.months,
                    v.days,
                    v.nanoseconds,
                ))
            })
        }
    }
}

/// Try to build create table request from insert data.
pub fn build_create_expr_from_insertion(
    catalog_name: &str,
    schema_name: &str,
    table_id: Option<TableId>,
    table_name: &str,
    columns: &[Column],
    engine: &str,
) -> Result<CreateTableExpr> {
    let column_exprs = ColumnExpr::from_columns(columns);
    build_create_table_expr(
        catalog_name,
        schema_name,
        table_id,
        table_name,
        column_exprs,
        engine,
        "Created on insertion",
    )
}

pub fn to_table_insert_request(
    catalog_name: &str,
    schema_name: &str,
    request: GrpcInsertRequest,
) -> Result<InsertRequest> {
    let table_name = &request.table_name;
    let row_count = request.row_count as usize;

    let mut columns_values = HashMap::with_capacity(request.columns.len());
    for Column {
        column_name,
        values,
        null_mask,
        datatype,
        ..
    } in request.columns
    {
        let Some(values) = values else { continue };

        let datatype: ConcreteDataType = ColumnDataTypeWrapper::try_new(datatype)
            .context(ColumnDataTypeSnafu)?
            .into();
        let vector = add_values_to_builder(datatype, values, row_count, null_mask)?;

        ensure!(
            columns_values.insert(column_name.clone(), vector).is_none(),
            ColumnAlreadyExistsSnafu {
                column: column_name
            }
        );
    }

    Ok(InsertRequest {
        catalog_name: catalog_name.to_string(),
        schema_name: schema_name.to_string(),
        table_name: table_name.to_string(),
        columns_values,
        region_number: request.region_number,
    })
}

pub(crate) fn add_values_to_builder(
    data_type: ConcreteDataType,
    values: Values,
    row_count: usize,
    null_mask: Vec<u8>,
) -> Result<VectorRef> {
    if null_mask.is_empty() {
        Ok(values_to_vector(&data_type, values))
    } else {
        let builder = &mut data_type.create_mutable_vector(row_count);
        let values = convert_values(&data_type, values);
        let null_mask = BitVec::from_vec(null_mask);
        ensure!(
            null_mask.count_ones() + values.len() == row_count,
            UnexpectedValuesLengthSnafu {
                reason: "If null_mask is not empty, the sum of the number of nulls and the length of values must be equal to row_count."
            }
        );

        let mut idx_of_values = 0;
        for idx in 0..row_count {
            match is_null(&null_mask, idx) {
                Some(true) => builder.push_null(),
                _ => {
                    builder
                        .try_push_value_ref(values[idx_of_values].as_value_ref())
                        .context(CreateVectorSnafu)?;
                    idx_of_values += 1
                }
            }
        }
        Ok(builder.to_vector())
    }
}

fn values_to_vector(data_type: &ConcreteDataType, values: Values) -> VectorRef {
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

fn convert_values(data_type: &ConcreteDataType, values: Values) -> Vec<Value> {
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

fn is_null(null_mask: &BitVec, idx: usize) -> Option<bool> {
    null_mask.get(idx).as_deref().copied()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::{assert_eq, vec};

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::column::Values;
    use api::v1::{Column, ColumnDataType, IntervalMonthDayNano, SemanticType};
    use common_base::BitVec;
    use common_catalog::consts::MITO_ENGINE;
    use common_time::interval::IntervalUnit;
    use common_time::timestamp::{TimeUnit, Timestamp};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder};
    use datatypes::types::{
        IntervalDayTimeType, IntervalMonthDayNanoType, IntervalYearMonthType, TimeMillisecondType,
        TimeSecondType, TimeType, TimestampMillisecondType, TimestampSecondType, TimestampType,
    };
    use datatypes::value::Value;
    use paste::paste;
    use snafu::ResultExt;

    use super::*;
    use crate::error::ColumnDataTypeSnafu;
    use crate::insert::find_new_columns;

    #[inline]
    fn build_column_schema(
        column_name: &str,
        datatype: i32,
        nullable: bool,
    ) -> Result<ColumnSchema> {
        let datatype_wrapper =
            ColumnDataTypeWrapper::try_new(datatype).context(ColumnDataTypeSnafu)?;

        Ok(ColumnSchema::new(
            column_name,
            datatype_wrapper.into(),
            nullable,
        ))
    }

    #[test]
    fn test_build_create_table_request() {
        let table_id = Some(10);
        let table_name = "test_metric";

        assert!(
            build_create_expr_from_insertion("", "", table_id, table_name, &[], MITO_ENGINE)
                .is_err()
        );

        let insert_batch = mock_insert_batch();

        let create_expr = build_create_expr_from_insertion(
            "",
            "",
            table_id,
            table_name,
            &insert_batch.0,
            MITO_ENGINE,
        )
        .unwrap();

        assert_eq!(table_id, create_expr.table_id.map(|x| x.id));
        assert_eq!(table_name, create_expr.table_name);
        assert_eq!("Created on insertion".to_string(), create_expr.desc);
        assert_eq!(
            vec![create_expr.column_defs[0].name.clone()],
            create_expr.primary_keys
        );

        let column_defs = create_expr.column_defs;
        assert_eq!(column_defs[5].name, create_expr.time_index);
        assert_eq!(6, column_defs.len());

        assert_eq!(
            ConcreteDataType::string_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "host")
                        .unwrap()
                        .datatype
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "cpu")
                        .unwrap()
                        .datatype
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "memory")
                        .unwrap()
                        .datatype
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "time")
                        .unwrap()
                        .datatype
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "interval")
                        .unwrap()
                        .datatype
                )
                .unwrap()
            )
        );

        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    column_defs
                        .iter()
                        .find(|c| c.name == "ts")
                        .unwrap()
                        .datatype
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn test_find_new_columns() {
        let mut columns = Vec::with_capacity(1);
        let cpu_column = build_column_schema("cpu", 10, true).unwrap();
        let ts_column = build_column_schema("ts", 15, false)
            .unwrap()
            .with_time_index(true);
        columns.push(cpu_column);
        columns.push(ts_column);

        let schema = Arc::new(SchemaBuilder::try_from(columns).unwrap().build().unwrap());

        assert!(find_new_columns(&schema, &[]).unwrap().is_none());

        let insert_batch = mock_insert_batch();

        let add_columns = find_new_columns(&schema, &insert_batch.0).unwrap().unwrap();

        assert_eq!(4, add_columns.add_columns.len());
        let host_column = &add_columns.add_columns[0];
        assert!(host_column.is_key);

        assert_eq!(
            ConcreteDataType::string_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(host_column.column_def.as_ref().unwrap().datatype)
                    .unwrap()
            )
        );

        let memory_column = &add_columns.add_columns[1];
        assert!(!memory_column.is_key);

        assert_eq!(
            ConcreteDataType::float64_datatype(),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(memory_column.column_def.as_ref().unwrap().datatype)
                    .unwrap()
            )
        );

        let time_column = &add_columns.add_columns[2];
        assert!(!time_column.is_key);

        assert_eq!(
            ConcreteDataType::time_datatype(TimeUnit::Millisecond),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(time_column.column_def.as_ref().unwrap().datatype)
                    .unwrap()
            )
        );

        let interval_column = &add_columns.add_columns[3];
        assert!(!interval_column.is_key);

        assert_eq!(
            ConcreteDataType::interval_datatype(IntervalUnit::MonthDayNano),
            ConcreteDataType::from(
                ColumnDataTypeWrapper::try_new(
                    interval_column.column_def.as_ref().unwrap().datatype
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn test_to_table_insert_request() {
        let (columns, row_count) = mock_insert_batch();
        let request = GrpcInsertRequest {
            table_name: "demo".to_string(),
            columns,
            row_count,
            region_number: 0,
        };
        let insert_req = to_table_insert_request("greptime", "public", request).unwrap();

        assert_eq!("greptime", insert_req.catalog_name);
        assert_eq!("public", insert_req.schema_name);
        assert_eq!("demo", insert_req.table_name);

        let host = insert_req.columns_values.get("host").unwrap();
        assert_eq!(Value::String("host1".into()), host.get(0));
        assert_eq!(Value::String("host2".into()), host.get(1));

        let cpu = insert_req.columns_values.get("cpu").unwrap();
        assert_eq!(Value::Float64(0.31.into()), cpu.get(0));
        assert_eq!(Value::Null, cpu.get(1));

        let memory = insert_req.columns_values.get("memory").unwrap();
        assert_eq!(Value::Null, memory.get(0));
        assert_eq!(Value::Float64(0.1.into()), memory.get(1));

        let ts = insert_req.columns_values.get("ts").unwrap();
        assert_eq!(Value::Timestamp(Timestamp::new_millisecond(100)), ts.get(0));
        assert_eq!(Value::Timestamp(Timestamp::new_millisecond(101)), ts.get(1));
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
                    let result = convert_values(&data_type, values);

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
    fn test_convert_timestamp_values() {
        // second
        let actual = convert_values(
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
        let actual = convert_values(
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
        let actual = convert_values(
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
        let actual = convert_values(
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
        let actual = convert_values(
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
        let actual = convert_values(
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
        let actual = convert_values(
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

    #[test]
    fn test_is_null() {
        let null_mask = BitVec::from_slice(&[0b0000_0001, 0b0000_1000]);

        assert_eq!(Some(true), is_null(&null_mask, 0));
        assert_eq!(Some(false), is_null(&null_mask, 1));
        assert_eq!(Some(false), is_null(&null_mask, 10));
        assert_eq!(Some(true), is_null(&null_mask, 11));
        assert_eq!(Some(false), is_null(&null_mask, 12));

        assert_eq!(None, is_null(&null_mask, 16));
        assert_eq!(None, is_null(&null_mask, 99));
    }

    fn mock_insert_batch() -> (Vec<Column>, u32) {
        let row_count = 2;

        let host_vals = Values {
            string_values: vec!["host1".to_string(), "host2".to_string()],
            ..Default::default()
        };
        let host_column = Column {
            column_name: "host".to_string(),
            semantic_type: SemanticType::Tag as i32,
            values: Some(host_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::String as i32,
        };

        let cpu_vals = Values {
            f64_values: vec![0.31],
            ..Default::default()
        };
        let cpu_column = Column {
            column_name: "cpu".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(cpu_vals),
            null_mask: vec![2],
            datatype: ColumnDataType::Float64 as i32,
        };

        let mem_vals = Values {
            f64_values: vec![0.1],
            ..Default::default()
        };
        let mem_column = Column {
            column_name: "memory".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(mem_vals),
            null_mask: vec![1],
            datatype: ColumnDataType::Float64 as i32,
        };

        let time_vals = Values {
            time_millisecond_values: vec![100, 101],
            ..Default::default()
        };
        let time_column = Column {
            column_name: "time".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(time_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::TimeMillisecond as i32,
        };

        let interval1 = IntervalMonthDayNano {
            months: 1,
            days: 2,
            nanoseconds: 3,
        };
        let interval2 = IntervalMonthDayNano {
            months: 4,
            days: 5,
            nanoseconds: 6,
        };
        let interval_vals = Values {
            interval_month_day_nano_values: vec![interval1, interval2],
            ..Default::default()
        };
        let interval_column = Column {
            column_name: "interval".to_string(),
            semantic_type: SemanticType::Field as i32,
            values: Some(interval_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::IntervalMonthDayNano as i32,
        };

        let ts_vals = Values {
            ts_millisecond_values: vec![100, 101],
            ..Default::default()
        };
        let ts_column = Column {
            column_name: "ts".to_string(),
            semantic_type: SemanticType::Timestamp as i32,
            values: Some(ts_vals),
            null_mask: vec![0],
            datatype: ColumnDataType::TimestampMillisecond as i32,
        };

        (
            vec![
                host_column,
                cpu_column,
                mem_column,
                time_column,
                interval_column,
                ts_column,
            ],
            row_count,
        )
    }
}
