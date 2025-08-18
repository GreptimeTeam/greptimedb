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

use chrono::NaiveDate;
use common_query::prelude::ScalarValue;
use common_sql::convert::sql_value_to_value;
use common_time::Timestamp;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::LogicalPlan;
use datatypes::prelude::ConcreteDataType;
use datatypes::types::TimestampType;
use datatypes::value::{self, Value};
use itertools::Itertools;
use opensrv_mysql::{to_naive_datetime, ParamValue, ValueInner};
use snafu::ResultExt;
use sql::ast::{visit_expressions_mut, Expr, Value as ValueExpr, ValueWithSpan, VisitMut};
use sql::statements::statement::Statement;

use crate::error::{self, DataFusionSnafu, Result};

/// Returns the placeholder string "$i".
pub fn format_placeholder(i: usize) -> String {
    format!("${}", i)
}

/// Replace all the "?" placeholder into "$i" in SQL,
/// returns the new SQL and the last placeholder index.
pub fn replace_placeholders(query: &str) -> (String, usize) {
    let query_parts = query.split('?').collect::<Vec<_>>();
    let parts_len = query_parts.len();
    let mut index = 0;
    let query = query_parts
        .into_iter()
        .enumerate()
        .map(|(i, part)| {
            if i == parts_len - 1 {
                return part.to_string();
            }

            index += 1;
            format!("{part}{}", format_placeholder(index))
        })
        .join("");

    (query, index + 1)
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

/// Give placeholder that cast to certain type `data_type` the same data type as is cast to
///
/// because it seems datafusion will not give data type to placeholder if it need to be cast to certain type, still unknown if this is a feature or a bug. And if a placeholder expr have no data type, datafusion will fail to extract it using `LogicalPlan::get_parameter_types`
pub fn fix_placeholder_types(plan: &mut LogicalPlan) -> Result<()> {
    let give_placeholder_types = |mut e: datafusion_expr::Expr| {
        if let datafusion_expr::Expr::Cast(cast) = &mut e {
            if let datafusion_expr::Expr::Placeholder(ph) = &mut *cast.expr {
                if ph.data_type.is_none() {
                    ph.data_type = Some(cast.data_type.clone());
                    common_telemetry::debug!(
                        "give placeholder type {:?} to {:?}",
                        cast.data_type,
                        ph
                    );
                    Ok(Transformed::yes(e))
                } else {
                    Ok(Transformed::no(e))
                }
            } else {
                Ok(Transformed::no(e))
            }
        } else {
            Ok(Transformed::no(e))
        }
    };
    let give_placeholder_types_recursively =
        |e: datafusion_expr::Expr| e.transform(give_placeholder_types);
    *plan = std::mem::take(plan)
        .transform(|p| p.map_expressions(give_placeholder_types_recursively))
        .context(DataFusionSnafu)?
        .data;
    Ok(())
}

fn visit_placeholders<V>(v: &mut V)
where
    V: VisitMut,
{
    let mut index = 1;
    let _ = visit_expressions_mut(v, |expr| {
        if let Expr::Value(ValueWithSpan {
            value: ValueExpr::Placeholder(s),
            ..
        }) = expr
        {
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
            ConcreteDataType::Boolean(_) => Ok(ScalarValue::Boolean(Some(i != 0))),
            ConcreteDataType::Timestamp(ts_type) => Value::Timestamp(ts_type.create_timestamp(i))
                .try_to_scalar_value(t)
                .context(error::ConvertScalarValueSnafu),

            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: param.coltype,
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
            ConcreteDataType::Boolean(_) => Ok(ScalarValue::Boolean(Some(u != 0))),
            ConcreteDataType::Timestamp(ts_type) => {
                Value::Timestamp(ts_type.create_timestamp(u as i64))
                    .try_to_scalar_value(t)
                    .context(error::ConvertScalarValueSnafu)
            }

            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: param.coltype,
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
                actual: param.coltype,
            }
            .fail(),
        },
        ValueInner::NULL => value::to_null_scalar_value(t).context(error::ConvertScalarValueSnafu),
        ValueInner::Bytes(b) => match t {
            ConcreteDataType::String(_) => Ok(ScalarValue::Utf8(Some(
                String::from_utf8_lossy(b).to_string(),
            ))),
            ConcreteDataType::Binary(_) => Ok(ScalarValue::Binary(Some(b.to_vec()))),
            ConcreteDataType::Timestamp(ts_type) => covert_bytes_to_timestamp(b, ts_type),
            _ => error::PreparedStmtTypeMismatchSnafu {
                expected: t,
                actual: param.coltype,
            }
            .fail(),
        },
        ValueInner::Date(_) => {
            let date: common_time::Date = NaiveDate::from(param.value).into();
            Ok(ScalarValue::Date32(Some(date.val())))
        }
        ValueInner::Datetime(_) => {
            let timestamp_millis = to_naive_datetime(param.value)
                .map_err(|e| {
                    error::MysqlValueConversionSnafu {
                        err_msg: e.to_string(),
                    }
                    .build()
                })?
                .and_utc()
                .timestamp_millis();

            match t {
                ConcreteDataType::Timestamp(_) => Ok(ScalarValue::TimestampMillisecond(
                    Some(timestamp_millis),
                    None,
                )),
                _ => error::PreparedStmtTypeMismatchSnafu {
                    expected: t,
                    actual: param.coltype,
                }
                .fail(),
            }
        }
        ValueInner::Time(_) => Ok(ScalarValue::Time64Nanosecond(Some(
            Duration::from(param.value).as_millis() as i64,
        ))),
    }
}

/// Convert an MySQL expression to a scalar value.
/// It automatically handles the conversion of strings to numeric values.
pub fn convert_expr_to_scalar_value(param: &Expr, t: &ConcreteDataType) -> Result<ScalarValue> {
    match param {
        Expr::Value(v) => {
            let v = sql_value_to_value("", t, &v.value, None, None, true);
            match v {
                Ok(v) => v
                    .try_to_scalar_value(t)
                    .context(error::ConvertScalarValueSnafu),
                Err(e) => error::InvalidParameterSnafu {
                    reason: e.to_string(),
                }
                .fail(),
            }
        }
        Expr::UnaryOp { op, expr } if let Expr::Value(v) = &**expr => {
            let v = sql_value_to_value("", t, &v.value, None, Some(*op), true);
            match v {
                Ok(v) => v
                    .try_to_scalar_value(t)
                    .context(error::ConvertScalarValueSnafu),
                Err(e) => error::InvalidParameterSnafu {
                    reason: e.to_string(),
                }
                .fail(),
            }
        }
        _ => error::InvalidParameterSnafu {
            reason: format!("cannot convert {:?} to scalar value of type {}", param, t),
        }
        .fail(),
    }
}

fn covert_bytes_to_timestamp(bytes: &[u8], ts_type: &TimestampType) -> Result<ScalarValue> {
    let ts = Timestamp::from_str_utc(&String::from_utf8_lossy(bytes))
        .map_err(|e| {
            error::MysqlValueConversionSnafu {
                err_msg: e.to_string(),
            }
            .build()
        })?
        .convert_to(ts_type.unit())
        .ok_or_else(|| {
            error::MysqlValueConversionSnafu {
                err_msg: "Overflow when converting timestamp to target unit".to_string(),
            }
            .build()
        })?;
    match ts_type {
        TimestampType::Nanosecond(_) => {
            Ok(ScalarValue::TimestampNanosecond(Some(ts.value()), None))
        }
        TimestampType::Microsecond(_) => {
            Ok(ScalarValue::TimestampMicrosecond(Some(ts.value()), None))
        }
        TimestampType::Millisecond(_) => {
            Ok(ScalarValue::TimestampMillisecond(Some(ts.value()), None))
        }
        TimestampType::Second(_) => Ok(ScalarValue::TimestampSecond(Some(ts.value()), None)),
    }
}

#[cfg(test)]
mod tests {
    use datatypes::types::{
        TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
        TimestampSecondType,
    };
    use sql::dialect::MySqlDialect;
    use sql::parser::{ParseOptions, ParserContext};

    use super::*;

    #[test]
    fn test_format_placeholder() {
        assert_eq!("$1", format_placeholder(1));
        assert_eq!("$3", format_placeholder(3));
    }

    #[test]
    fn test_replace_placeholders() {
        let create = "create table demo(host string, ts timestamp time index)";
        let (sql, index) = replace_placeholders(create);
        assert_eq!(create, sql);
        assert_eq!(1, index);

        let insert = "insert into demo values(?,?,?)";
        let (sql, index) = replace_placeholders(insert);
        assert_eq!("insert into demo values($1,$2,$3)", sql);
        assert_eq!(4, index);

        let query = "select from demo where host=? and idc in (select idc from idcs where name=?) and cpu>?";
        let (sql, index) = replace_placeholders(query);
        assert_eq!("select from demo where host=$1 and idc in (select idc from idcs where name=$2) and cpu>$3", sql);
        assert_eq!(4, index);
    }

    fn parse_sql(sql: &str) -> Statement {
        let mut stmts =
            ParserContext::create_with_dialect(sql, &MySqlDialect {}, ParseOptions::default())
                .unwrap();
        stmts.remove(0)
    }

    #[test]
    fn test_transform_placeholders() {
        let insert = parse_sql("insert into demo values(?,?,?)");
        let Statement::Insert(insert) = transform_placeholders(insert) else {
            unreachable!()
        };
        assert_eq!(
            "INSERT INTO demo VALUES ($1, $2, $3)",
            insert.inner.to_string()
        );

        let delete = parse_sql("delete from demo where host=? and idc=?");
        let Statement::Delete(delete) = transform_placeholders(delete) else {
            unreachable!()
        };
        assert_eq!(
            "DELETE FROM demo WHERE host = $1 AND idc = $2",
            delete.inner.to_string()
        );

        let select = parse_sql("select * from demo where host=? and idc in (select idc from idcs where name=?) and cpu>?");
        let Statement::Query(select) = transform_placeholders(select) else {
            unreachable!()
        };
        assert_eq!("SELECT * FROM demo WHERE host = $1 AND idc IN (SELECT idc FROM idcs WHERE name = $2) AND cpu > $3", select.inner.to_string());
    }

    #[test]
    fn test_convert_expr_to_scalar_value() {
        let expr = Expr::Value(ValueExpr::Number("123".to_string(), false).into());
        let t = ConcreteDataType::int32_datatype();
        let v = convert_expr_to_scalar_value(&expr, &t).unwrap();
        assert_eq!(ScalarValue::Int32(Some(123)), v);

        let expr = Expr::Value(ValueExpr::Number("123.456789".to_string(), false).into());
        let t = ConcreteDataType::float64_datatype();
        let v = convert_expr_to_scalar_value(&expr, &t).unwrap();
        assert_eq!(ScalarValue::Float64(Some(123.456789)), v);

        let expr = Expr::Value(ValueExpr::SingleQuotedString("2001-01-02".to_string()).into());
        let t = ConcreteDataType::date_datatype();
        let v = convert_expr_to_scalar_value(&expr, &t).unwrap();
        let scalar_v = ScalarValue::Utf8(Some("2001-01-02".to_string()))
            .cast_to(&arrow_schema::DataType::Date32)
            .unwrap();
        assert_eq!(scalar_v, v);

        let expr =
            Expr::Value(ValueExpr::SingleQuotedString("2001-01-02 03:04:05".to_string()).into());
        let t = ConcreteDataType::timestamp_microsecond_datatype();
        let v = convert_expr_to_scalar_value(&expr, &t).unwrap();
        let scalar_v = ScalarValue::Utf8(Some("2001-01-02 03:04:05".to_string()))
            .cast_to(&arrow_schema::DataType::Timestamp(
                arrow_schema::TimeUnit::Microsecond,
                None,
            ))
            .unwrap();
        assert_eq!(scalar_v, v);

        let expr = Expr::Value(ValueExpr::SingleQuotedString("hello".to_string()).into());
        let t = ConcreteDataType::string_datatype();
        let v = convert_expr_to_scalar_value(&expr, &t).unwrap();
        assert_eq!(ScalarValue::Utf8(Some("hello".to_string())), v);

        let expr = Expr::Value(ValueExpr::Null.into());
        let t = ConcreteDataType::time_microsecond_datatype();
        let v = convert_expr_to_scalar_value(&expr, &t).unwrap();
        assert_eq!(ScalarValue::Time64Microsecond(None), v);
    }

    #[test]
    fn test_convert_bytes_to_timestamp() {
        let test_cases = vec![
            // input unix timestamp in seconds -> nanosecond.
            (
                "2024-12-26 12:00:00",
                TimestampType::Nanosecond(TimestampNanosecondType),
                ScalarValue::TimestampNanosecond(Some(1735214400000000000), None),
            ),
            // input unix timestamp in seconds -> microsecond.
            (
                "2024-12-26 12:00:00",
                TimestampType::Microsecond(TimestampMicrosecondType),
                ScalarValue::TimestampMicrosecond(Some(1735214400000000), None),
            ),
            // input unix timestamp in seconds -> millisecond.
            (
                "2024-12-26 12:00:00",
                TimestampType::Millisecond(TimestampMillisecondType),
                ScalarValue::TimestampMillisecond(Some(1735214400000), None),
            ),
            // input unix timestamp in seconds -> second.
            (
                "2024-12-26 12:00:00",
                TimestampType::Second(TimestampSecondType),
                ScalarValue::TimestampSecond(Some(1735214400), None),
            ),
            // input unix timestamp in milliseconds -> nanosecond.
            (
                "2024-12-26 12:00:00.123",
                TimestampType::Nanosecond(TimestampNanosecondType),
                ScalarValue::TimestampNanosecond(Some(1735214400123000000), None),
            ),
            // input unix timestamp in milliseconds -> microsecond.
            (
                "2024-12-26 12:00:00.123",
                TimestampType::Microsecond(TimestampMicrosecondType),
                ScalarValue::TimestampMicrosecond(Some(1735214400123000), None),
            ),
            // input unix timestamp in milliseconds -> millisecond.
            (
                "2024-12-26 12:00:00.123",
                TimestampType::Millisecond(TimestampMillisecondType),
                ScalarValue::TimestampMillisecond(Some(1735214400123), None),
            ),
            // input unix timestamp in milliseconds -> second.
            (
                "2024-12-26 12:00:00.123",
                TimestampType::Second(TimestampSecondType),
                ScalarValue::TimestampSecond(Some(1735214400), None),
            ),
            // input unix timestamp in microseconds -> nanosecond.
            (
                "2024-12-26 12:00:00.123456",
                TimestampType::Nanosecond(TimestampNanosecondType),
                ScalarValue::TimestampNanosecond(Some(1735214400123456000), None),
            ),
            // input unix timestamp in microseconds -> microsecond.
            (
                "2024-12-26 12:00:00.123456",
                TimestampType::Microsecond(TimestampMicrosecondType),
                ScalarValue::TimestampMicrosecond(Some(1735214400123456), None),
            ),
            // input unix timestamp in microseconds -> millisecond.
            (
                "2024-12-26 12:00:00.123456",
                TimestampType::Millisecond(TimestampMillisecondType),
                ScalarValue::TimestampMillisecond(Some(1735214400123), None),
            ),
            // input unix timestamp in milliseconds -> second.
            (
                "2024-12-26 12:00:00.123456",
                TimestampType::Second(TimestampSecondType),
                ScalarValue::TimestampSecond(Some(1735214400), None),
            ),
        ];

        for (input, ts_type, expected) in test_cases {
            let result = covert_bytes_to_timestamp(input.as_bytes(), &ts_type).unwrap();
            assert_eq!(result, expected);
        }
    }
}
