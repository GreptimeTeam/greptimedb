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
use std::ops::ControlFlow;
use std::time::Duration;

use chrono::{NaiveDate, NaiveDateTime};
use common_query::prelude::ScalarValue;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{self, Value};
use opensrv_mysql::{ParamValue, ValueInner};
use snafu::ResultExt;
use sql::ast::{visit_expressions_mut, Expr, Value as ValueExpr, VisitMut};
use sql::statements::statement::Statement;

use crate::error::{self, Result};

/// Returns the placeholder string "$i".
pub fn format_placeholder(i: usize) -> String {
    format!("${}", i)
}

/// Relace all the "?" placeholder into "$i" in SQL,
/// returns the new SQL and the last placeholder index.
pub fn replace_placeholder(query: &str) -> (String, usize) {
    let mut query = query.to_string();
    let mut index = 1;
    let mut offset = 0;
    while let Some(position) = query[offset..].find('?') {
        let place_holder = format!("${}", index);
        let position = position + offset;
        query.replace_range(position..position + 1, &place_holder);
        index += 1;
        offset = position + 1;
    }
    (query, index)
}

/// Transform all the "?" placeholder into "$i".
/// Only works for Insert,Query and Delete statements.
pub fn transform_placeholders(stmt: Statement) -> Statement {
    match stmt {
        Statement::Query(mut query) => {
            visit_placeholders(&mut query.inner);
            Statement::Query(query)
        }
        Statement::Insert(mut insert) => {
            visit_placeholders(&mut insert.inner);
            Statement::Insert(insert)
        }
        Statement::Delete(mut delete) => {
            visit_placeholders(&mut delete.inner);
            Statement::Delete(delete)
        }
        stmt => stmt,
    }
}

fn visit_placeholders<V>(v: &mut V)
where
    V: VisitMut,
{
    let mut index = 1;
    visit_expressions_mut(v, |expr| {
        if let Expr::Value(ValueExpr::Placeholder(s)) = expr {
            *s = format_placeholder(index);
            index += 1;
        }
        ControlFlow::<()>::Continue(())
    });
}

/// Convert [`ParamValue`] into [`Value`] according to param type.
/// It will try it's best to do type conversions if possible
pub fn convert_value(param: &ParamValue, t: &ConcreteDataType) -> Result<ScalarValue> {
    match param.value.into_inner() {
        ValueInner::Int(i) => match t {
            ConcreteDataType::Int8(_) => Ok(ScalarValue::Int8(Some(i as i8))),
            ConcreteDataType::Int16(_) => Ok(ScalarValue::Int16(Some(i as i16))),
            ConcreteDataType::Int32(_) => Ok(ScalarValue::Int32(Some(i as i32))),
            ConcreteDataType::Int64(_) => Ok(ScalarValue::Int64(Some(i))),
            ConcreteDataType::UInt8(_) => Ok(ScalarValue::UInt8(Some(i as u8))),
            ConcreteDataType::UInt16(_) => Ok(ScalarValue::UInt16(Some(i as u16))),
            ConcreteDataType::UInt32(_) => Ok(ScalarValue::UInt32(Some(i as u32))),
            ConcreteDataType::UInt64(_) => Ok(ScalarValue::UInt64(Some(i as u64))),
            ConcreteDataType::Float32(_) => Ok(ScalarValue::Float32(Some(i as f32))),
            ConcreteDataType::Float64(_) => Ok(ScalarValue::Float64(Some(i as f64))),
            ConcreteDataType::Timestamp(ts_type) => Value::Timestamp(ts_type.create_timestamp(i))
                .try_to_scalar_value(t)
                .context(error::ConvertScalarValueSnafu),

            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: ConcreteDataType::int8_datatype(),
            }
            .fail(),
        },
        ValueInner::UInt(u) => match t {
            ConcreteDataType::Int8(_) => Ok(ScalarValue::Int8(Some(u as i8))),
            ConcreteDataType::Int16(_) => Ok(ScalarValue::Int16(Some(u as i16))),
            ConcreteDataType::Int32(_) => Ok(ScalarValue::Int32(Some(u as i32))),
            ConcreteDataType::Int64(_) => Ok(ScalarValue::Int64(Some(u as i64))),
            ConcreteDataType::UInt8(_) => Ok(ScalarValue::UInt8(Some(u as u8))),
            ConcreteDataType::UInt16(_) => Ok(ScalarValue::UInt16(Some(u as u16))),
            ConcreteDataType::UInt32(_) => Ok(ScalarValue::UInt32(Some(u as u32))),
            ConcreteDataType::UInt64(_) => Ok(ScalarValue::UInt64(Some(u))),
            ConcreteDataType::Float32(_) => Ok(ScalarValue::Float32(Some(u as f32))),
            ConcreteDataType::Float64(_) => Ok(ScalarValue::Float64(Some(u as f64))),
            ConcreteDataType::Timestamp(ts_type) => {
                Value::Timestamp(ts_type.create_timestamp(u as i64))
                    .try_to_scalar_value(t)
                    .context(error::ConvertScalarValueSnafu)
            }

            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: ConcreteDataType::uint8_datatype(),
            }
            .fail(),
        },
        ValueInner::Double(f) => match t {
            ConcreteDataType::Int8(_) => Ok(ScalarValue::Int8(Some(f as i8))),
            ConcreteDataType::Int16(_) => Ok(ScalarValue::Int16(Some(f as i16))),
            ConcreteDataType::Int32(_) => Ok(ScalarValue::Int32(Some(f as i32))),
            ConcreteDataType::Int64(_) => Ok(ScalarValue::Int64(Some(f as i64))),
            ConcreteDataType::UInt8(_) => Ok(ScalarValue::UInt8(Some(f as u8))),
            ConcreteDataType::UInt16(_) => Ok(ScalarValue::UInt16(Some(f as u16))),
            ConcreteDataType::UInt32(_) => Ok(ScalarValue::UInt32(Some(f as u32))),
            ConcreteDataType::UInt64(_) => Ok(ScalarValue::UInt64(Some(f as u64))),
            ConcreteDataType::Float32(_) => Ok(ScalarValue::Float32(Some(f as f32))),
            ConcreteDataType::Float64(_) => Ok(ScalarValue::Float64(Some(f))),

            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: ConcreteDataType::float64_datatype(),
            }
            .fail(),
        },
        ValueInner::NULL => Ok(value::to_null_scalar_value(t)),
        ValueInner::Bytes(b) => match t {
            ConcreteDataType::String(_) => Ok(ScalarValue::Utf8(Some(
                String::from_utf8_lossy(b).to_string(),
            ))),
            ConcreteDataType::Binary(_) => Ok(ScalarValue::LargeBinary(Some(b.to_vec()))),

            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: ConcreteDataType::binary_datatype(),
            }
            .fail(),
        },
        ValueInner::Date(_) => {
            let date: common_time::Date = NaiveDate::from(param.value).into();
            Ok(ScalarValue::Date32(Some(date.val())))
        }
        ValueInner::Datetime(_) => Ok(ScalarValue::Date64(Some(
            NaiveDateTime::from(param.value).timestamp_millis(),
        ))),
        ValueInner::Time(_) => Ok(ScalarValue::Time64Nanosecond(Some(
            Duration::from(param.value).as_millis() as i64,
        ))),
    }
}
