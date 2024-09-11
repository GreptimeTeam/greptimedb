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

pub mod alter;
pub mod copy;
pub mod create;
pub mod delete;
pub mod describe;
pub mod drop;
pub mod explain;
pub mod insert;
mod option_map;
pub mod query;
pub mod set_variables;
pub mod show;
pub mod statement;
pub mod tql;
mod transform;
pub mod truncate;

use std::str::FromStr;

use api::helper::ColumnDataTypeWrapper;
use api::v1::add_column_location::LocationType;
use api::v1::{AddColumnLocation as Location, SemanticType};
use common_base::bytes::Bytes;
use common_query::AddColumnLocation;
use common_time::timezone::Timezone;
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::constraint::{CURRENT_TIMESTAMP, CURRENT_TIMESTAMP_FN};
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, COMMENT_KEY};
use datatypes::types::{cast, TimestampType};
use datatypes::value::{OrderedF32, OrderedF64, Value};
use snafu::{ensure, OptionExt, ResultExt};
use sqlparser::ast::{ExactNumberInfo, UnaryOperator};

use crate::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType as SqlDataType, Expr, TimezoneInfo,
    Value as SqlValue,
};
use crate::error::{
    self, ColumnTypeMismatchSnafu, ConvertSqlValueSnafu, ConvertToGrpcDataTypeSnafu,
    ConvertValueSnafu, InvalidCastSnafu, InvalidSqlValueSnafu, InvalidUnaryOpSnafu,
    ParseSqlValueSnafu, Result, SerializeColumnDefaultConstraintSnafu, SetFulltextOptionSnafu,
    TimestampOverflowSnafu, UnsupportedDefaultValueSnafu, UnsupportedUnaryOpSnafu,
};
use crate::statements::create::Column;
pub use crate::statements::option_map::OptionMap;
pub use crate::statements::transform::{get_data_type_by_alias_name, transform_statements};

fn parse_string_to_value(
    column_name: &str,
    s: String,
    data_type: &ConcreteDataType,
    timezone: Option<&Timezone>,
) -> Result<Value> {
    ensure!(
        data_type.is_stringifiable(),
        ColumnTypeMismatchSnafu {
            column_name,
            expect: data_type.clone(),
            actual: ConcreteDataType::string_datatype(),
        }
    );

    match data_type {
        ConcreteDataType::String(_) => Ok(Value::String(s.into())),
        ConcreteDataType::Date(_) => {
            if let Ok(date) = common_time::date::Date::from_str(&s, timezone) {
                Ok(Value::Date(date))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Failed to parse {s} to Date value"),
                }
                .fail()
            }
        }
        ConcreteDataType::DateTime(_) => {
            if let Ok(datetime) = common_time::datetime::DateTime::from_str(&s, timezone) {
                Ok(Value::DateTime(datetime))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Failed to parse {s} to DateTime value"),
                }
                .fail()
            }
        }
        ConcreteDataType::Timestamp(t) => {
            if let Ok(ts) = Timestamp::from_str(&s, timezone) {
                Ok(Value::Timestamp(ts.convert_to(t.unit()).context(
                    TimestampOverflowSnafu {
                        timestamp: ts,
                        target_unit: t.unit(),
                    },
                )?))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Failed to parse {s} to Timestamp value"),
                }
                .fail()
            }
        }
        ConcreteDataType::Decimal128(_) => {
            if let Ok(val) = common_decimal::Decimal128::from_str(&s) {
                Ok(Value::Decimal128(val))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Fail to parse number {s} to Decimal128 value"),
                }
                .fail()
            }
        }
        ConcreteDataType::Binary(_) => Ok(Value::Binary(s.as_bytes().into())),
        _ => {
            unreachable!()
        }
    }
}

fn parse_hex_string(s: &str) -> Result<Value> {
    match hex::decode(s) {
        Ok(b) => Ok(Value::Binary(Bytes::from(b))),
        Err(hex::FromHexError::InvalidHexCharacter { c, index }) => ParseSqlValueSnafu {
            msg: format!(
                "Fail to parse hex string to Byte: invalid character {c:?} at position {index}"
            ),
        }
        .fail(),
        Err(hex::FromHexError::OddLength) => ParseSqlValueSnafu {
            msg: "Fail to parse hex string to Byte: odd number of digits".to_string(),
        }
        .fail(),
        Err(e) => ParseSqlValueSnafu {
            msg: format!("Fail to parse hex string to Byte {s}, {e:?}"),
        }
        .fail(),
    }
}

macro_rules! parse_number_to_value {
    ($data_type: expr, $n: ident,  $(($Type: ident, $PrimitiveType: ident, $Target: ident)), +) => {
        match $data_type {
            $(
                ConcreteDataType::$Type(_) => {
                    let n  = parse_sql_number::<$PrimitiveType>($n)?;
                    Ok(Value::$Type($Target::from(n)))
                },
            )+
            ConcreteDataType::Timestamp(t) => {
                let n  = parse_sql_number::<i64>($n)?;
                Ok(Value::Timestamp(Timestamp::new(n, t.unit())))
            },
            // TODO(QuenKar): This could need to be optimized
            // if this from_str function is slow,
            // we can implement parse decimal string with precision and scale manually.
            ConcreteDataType::Decimal128(_) => {
                if let Ok(val) = common_decimal::Decimal128::from_str($n) {
                    Ok(Value::Decimal128(val))
                } else {
                    ParseSqlValueSnafu {
                        msg: format!("Fail to parse number {}, invalid column type: {:?}",
                                        $n, $data_type)
                    }.fail()
                }
            }
            // It's valid for MySQL JDBC to send "0" and "1" for boolean types, so adapt to that.
            ConcreteDataType::Boolean(_) => {
                match $n {
                    "0" => Ok(Value::Boolean(false)),
                    "1" => Ok(Value::Boolean(true)),
                    _ => ParseSqlValueSnafu {
                        msg: format!("Failed to parse number '{}' to boolean column type", $n)}.fail(),
                }
            }
            _ => ParseSqlValueSnafu {
                msg: format!("Fail to parse number {}, invalid column type: {:?}",
                                $n, $data_type
                )}.fail(),
        }
    }
}

/// Convert a sql value into datatype's value
pub fn sql_number_to_value(data_type: &ConcreteDataType, n: &str) -> Result<Value> {
    parse_number_to_value!(
        data_type,
        n,
        (UInt8, u8, u8),
        (UInt16, u16, u16),
        (UInt32, u32, u32),
        (UInt64, u64, u64),
        (Int8, i8, i8),
        (Int16, i16, i16),
        (Int32, i32, i32),
        (Int64, i64, i64),
        (Float64, f64, OrderedF64),
        (Float32, f32, OrderedF32)
    )
    // TODO(hl): also Date/DateTime
}

fn parse_sql_number<R: FromStr + std::fmt::Debug>(n: &str) -> Result<R>
where
    <R as FromStr>::Err: std::fmt::Debug,
{
    match n.parse::<R>() {
        Ok(n) => Ok(n),
        Err(e) => ParseSqlValueSnafu {
            msg: format!("Fail to parse number {n}, {e:?}"),
        }
        .fail(),
    }
}

pub fn sql_value_to_value(
    column_name: &str,
    data_type: &ConcreteDataType,
    sql_val: &SqlValue,
    timezone: Option<&Timezone>,
    unary_op: Option<UnaryOperator>,
) -> Result<Value> {
    let mut value = match sql_val {
        SqlValue::Number(n, _) => sql_number_to_value(data_type, n)?,
        SqlValue::Null => Value::Null,
        SqlValue::Boolean(b) => {
            ensure!(
                data_type.is_boolean(),
                ColumnTypeMismatchSnafu {
                    column_name,
                    expect: data_type.clone(),
                    actual: ConcreteDataType::boolean_datatype(),
                }
            );

            (*b).into()
        }
        SqlValue::DoubleQuotedString(s) | SqlValue::SingleQuotedString(s) => {
            parse_string_to_value(column_name, s.clone(), data_type, timezone)?
        }
        SqlValue::HexStringLiteral(s) => parse_hex_string(s)?,
        SqlValue::Placeholder(s) => return InvalidSqlValueSnafu { value: s }.fail(),

        // TODO(dennis): supports binary string
        _ => {
            return ConvertSqlValueSnafu {
                value: sql_val.clone(),
                datatype: data_type.clone(),
            }
            .fail()
        }
    };

    if let Some(unary_op) = unary_op {
        match unary_op {
            UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not => {}
            UnaryOperator::PGBitwiseNot
            | UnaryOperator::PGSquareRoot
            | UnaryOperator::PGCubeRoot
            | UnaryOperator::PGPostfixFactorial
            | UnaryOperator::PGPrefixFactorial
            | UnaryOperator::PGAbs => {
                return UnsupportedUnaryOpSnafu { unary_op }.fail();
            }
        }

        match value {
            Value::Null => {}
            Value::Boolean(bool) => match unary_op {
                UnaryOperator::Not => value = Value::Boolean(!bool),
                _ => {
                    return InvalidUnaryOpSnafu { unary_op, value }.fail();
                }
            },
            Value::UInt8(_)
            | Value::UInt16(_)
            | Value::UInt32(_)
            | Value::UInt64(_)
            | Value::Int8(_)
            | Value::Int16(_)
            | Value::Int32(_)
            | Value::Int64(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Decimal128(_)
            | Value::Date(_)
            | Value::DateTime(_)
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Duration(_)
            | Value::Interval(_) => match unary_op {
                UnaryOperator::Plus => {}
                UnaryOperator::Minus => {
                    value = value
                        .try_negative()
                        .with_context(|| InvalidUnaryOpSnafu { unary_op, value })?;
                }
                _ => return InvalidUnaryOpSnafu { unary_op, value }.fail(),
            },

            Value::String(_) | Value::Binary(_) | Value::List(_) => {
                return InvalidUnaryOpSnafu { unary_op, value }.fail()
            }
        }
    }

    if value.data_type() != *data_type {
        cast(value, data_type).with_context(|_| InvalidCastSnafu {
            sql_value: sql_val.clone(),
            datatype: data_type,
        })
    } else {
        Ok(value)
    }
}

pub fn value_to_sql_value(val: &Value) -> Result<SqlValue> {
    Ok(match val {
        Value::Int8(v) => SqlValue::Number(v.to_string(), false),
        Value::UInt8(v) => SqlValue::Number(v.to_string(), false),
        Value::Int16(v) => SqlValue::Number(v.to_string(), false),
        Value::UInt16(v) => SqlValue::Number(v.to_string(), false),
        Value::Int32(v) => SqlValue::Number(v.to_string(), false),
        Value::UInt32(v) => SqlValue::Number(v.to_string(), false),
        Value::Int64(v) => SqlValue::Number(v.to_string(), false),
        Value::UInt64(v) => SqlValue::Number(v.to_string(), false),
        Value::Float32(v) => SqlValue::Number(v.to_string(), false),
        Value::Float64(v) => SqlValue::Number(v.to_string(), false),
        Value::Boolean(b) => SqlValue::Boolean(*b),
        Value::Date(d) => SqlValue::SingleQuotedString(d.to_string()),
        Value::DateTime(d) => SqlValue::SingleQuotedString(d.to_string()),
        Value::Timestamp(ts) => SqlValue::SingleQuotedString(ts.to_iso8601_string()),
        Value::String(s) => SqlValue::SingleQuotedString(s.as_utf8().to_string()),
        Value::Null => SqlValue::Null,
        // TODO(dennis): supports binary
        _ => return ConvertValueSnafu { value: val.clone() }.fail(),
    })
}

fn parse_column_default_constraint(
    column_name: &str,
    data_type: &ConcreteDataType,
    opts: &[ColumnOptionDef],
    timezone: Option<&Timezone>,
) -> Result<Option<ColumnDefaultConstraint>> {
    if let Some(opt) = opts
        .iter()
        .find(|o| matches!(o.option, ColumnOption::Default(_)))
    {
        let default_constraint = match &opt.option {
            ColumnOption::Default(Expr::Value(v)) => ColumnDefaultConstraint::Value(
                sql_value_to_value(column_name, data_type, v, timezone, None)?,
            ),
            ColumnOption::Default(Expr::Function(func)) => {
                let mut func = format!("{func}").to_lowercase();
                // normalize CURRENT_TIMESTAMP to CURRENT_TIMESTAMP()
                if func == CURRENT_TIMESTAMP {
                    func = CURRENT_TIMESTAMP_FN.to_string();
                }
                // Always use lowercase for function expression
                ColumnDefaultConstraint::Function(func.to_lowercase())
            }

            ColumnOption::Default(Expr::UnaryOp { op, expr }) => {
                // Specialized process for handling numerical inputs to prevent
                // overflow errors during the parsing of negative numbers,
                // See https://github.com/GreptimeTeam/greptimedb/issues/4351
                if let (UnaryOperator::Minus, Expr::Value(SqlValue::Number(n, _))) =
                    (op, expr.as_ref())
                {
                    return Ok(Some(ColumnDefaultConstraint::Value(sql_number_to_value(
                        data_type,
                        &format!("-{n}"),
                    )?)));
                }

                if let Expr::Value(v) = &**expr {
                    let value = sql_value_to_value(column_name, data_type, v, timezone, Some(*op))?;
                    ColumnDefaultConstraint::Value(value)
                } else {
                    return UnsupportedDefaultValueSnafu {
                        column_name,
                        expr: *expr.clone(),
                    }
                    .fail();
                }
            }
            ColumnOption::Default(others) => {
                return UnsupportedDefaultValueSnafu {
                    column_name,
                    expr: others.clone(),
                }
                .fail();
            }
            _ => unreachable!(),
        };

        Ok(Some(default_constraint))
    } else {
        Ok(None)
    }
}

/// Return true when the `ColumnDef` options contain primary key
pub fn has_primary_key_option(column_def: &ColumnDef) -> bool {
    column_def
        .options
        .iter()
        .any(|options| match options.option {
            ColumnOption::Unique { is_primary, .. } => is_primary,
            _ => false,
        })
}

// TODO(yingwen): Make column nullable by default, and checks invalid case like
// a column is not nullable but has a default value null.
/// Create a `ColumnSchema` from `Column`.
pub fn column_to_schema(
    column: &Column,
    is_time_index: bool,
    timezone: Option<&Timezone>,
) -> Result<ColumnSchema> {
    let is_nullable = column
        .options()
        .iter()
        .all(|o| !matches!(o.option, ColumnOption::NotNull))
        && !is_time_index;

    let name = column.name().value.clone();
    let data_type = sql_data_type_to_concrete_data_type(column.data_type())?;
    let default_constraint =
        parse_column_default_constraint(&name, &data_type, column.options(), timezone)?;

    let mut column_schema = ColumnSchema::new(name, data_type, is_nullable)
        .with_time_index(is_time_index)
        .with_default_constraint(default_constraint)
        .context(error::InvalidDefaultSnafu {
            column: &column.name().value,
        })?;

    if let Some(ColumnOption::Comment(c)) = column.options().iter().find_map(|o| {
        if matches!(o.option, ColumnOption::Comment(_)) {
            Some(&o.option)
        } else {
            None
        }
    }) {
        let _ = column_schema
            .mut_metadata()
            .insert(COMMENT_KEY.to_string(), c.to_string());
    }

    if let Some(options) = column.extensions.build_fulltext_options()? {
        column_schema = column_schema
            .with_fulltext_options(options)
            .context(SetFulltextOptionSnafu)?;
    }

    Ok(column_schema)
}

/// Convert `ColumnDef` in sqlparser to `ColumnDef` in gRPC proto.
pub fn sql_column_def_to_grpc_column_def(
    col: &ColumnDef,
    timezone: Option<&Timezone>,
) -> Result<api::v1::ColumnDef> {
    let name = col.name.value.clone();
    let data_type = sql_data_type_to_concrete_data_type(&col.data_type)?;

    let is_nullable = col
        .options
        .iter()
        .all(|o| !matches!(o.option, ColumnOption::NotNull));

    let default_constraint =
        parse_column_default_constraint(&name, &data_type, &col.options, timezone)?
            .map(ColumnDefaultConstraint::try_into) // serialize default constraint to bytes
            .transpose()
            .context(SerializeColumnDefaultConstraintSnafu)?;
    // convert ConcreteDataType to grpc ColumnDataTypeWrapper
    let (datatype, datatype_ext) = ColumnDataTypeWrapper::try_from(data_type.clone())
        .context(ConvertToGrpcDataTypeSnafu)?
        .to_parts();

    let is_primary_key = col.options.iter().any(|o| {
        matches!(
            o.option,
            ColumnOption::Unique {
                is_primary: true,
                ..
            }
        )
    });

    let semantic_type = if is_primary_key {
        SemanticType::Tag
    } else {
        SemanticType::Field
    };

    Ok(api::v1::ColumnDef {
        name,
        data_type: datatype as i32,
        is_nullable,
        default_constraint: default_constraint.unwrap_or_default(),
        semantic_type: semantic_type as _,
        comment: String::new(),
        datatype_extension: datatype_ext,
        options: None,
    })
}

pub fn sql_data_type_to_concrete_data_type(data_type: &SqlDataType) -> Result<ConcreteDataType> {
    match data_type {
        SqlDataType::BigInt(_) | SqlDataType::Int64 => Ok(ConcreteDataType::int64_datatype()),
        SqlDataType::UnsignedBigInt(_) => Ok(ConcreteDataType::uint64_datatype()),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(ConcreteDataType::int32_datatype()),
        SqlDataType::UnsignedInt(_) | SqlDataType::UnsignedInteger(_) => {
            Ok(ConcreteDataType::uint32_datatype())
        }
        SqlDataType::SmallInt(_) => Ok(ConcreteDataType::int16_datatype()),
        SqlDataType::UnsignedSmallInt(_) => Ok(ConcreteDataType::uint16_datatype()),
        SqlDataType::TinyInt(_) | SqlDataType::Int8(_) => Ok(ConcreteDataType::int8_datatype()),
        SqlDataType::UnsignedTinyInt(_) | SqlDataType::UnsignedInt8(_) => {
            Ok(ConcreteDataType::uint8_datatype())
        }
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String(_) => Ok(ConcreteDataType::string_datatype()),
        SqlDataType::Float(_) => Ok(ConcreteDataType::float32_datatype()),
        SqlDataType::Double | SqlDataType::Float64 => Ok(ConcreteDataType::float64_datatype()),
        SqlDataType::Boolean => Ok(ConcreteDataType::boolean_datatype()),
        SqlDataType::Date => Ok(ConcreteDataType::date_datatype()),
        SqlDataType::Binary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Bytea
        | SqlDataType::Varbinary(_) => Ok(ConcreteDataType::binary_datatype()),
        SqlDataType::Datetime(_) => Ok(ConcreteDataType::datetime_datatype()),
        SqlDataType::Timestamp(precision, _) => Ok(precision
            .as_ref()
            .map(|v| TimestampType::try_from(*v))
            .transpose()
            .map_err(|_| {
                error::SqlTypeNotSupportedSnafu {
                    t: data_type.clone(),
                }
                .build()
            })?
            .map(|t| ConcreteDataType::timestamp_datatype(t.unit()))
            .unwrap_or(ConcreteDataType::timestamp_millisecond_datatype())),
        SqlDataType::Interval => Ok(ConcreteDataType::interval_month_day_nano_datatype()),
        SqlDataType::Decimal(exact_info) => match exact_info {
            ExactNumberInfo::None => Ok(ConcreteDataType::decimal128_default_datatype()),
            // refer to https://dev.mysql.com/doc/refman/8.0/en/fixed-point-types.html
            // In standard SQL, the syntax DECIMAL(M) is equivalent to DECIMAL(M,0).
            ExactNumberInfo::Precision(p) => Ok(ConcreteDataType::decimal128_datatype(*p as u8, 0)),
            ExactNumberInfo::PrecisionAndScale(p, s) => {
                Ok(ConcreteDataType::decimal128_datatype(*p as u8, *s as i8))
            }
        },
        _ => error::SqlTypeNotSupportedSnafu {
            t: data_type.clone(),
        }
        .fail(),
    }
}

pub fn concrete_data_type_to_sql_data_type(data_type: &ConcreteDataType) -> Result<SqlDataType> {
    match data_type {
        ConcreteDataType::Int64(_) => Ok(SqlDataType::BigInt(None)),
        ConcreteDataType::UInt64(_) => Ok(SqlDataType::UnsignedBigInt(None)),
        ConcreteDataType::Int32(_) => Ok(SqlDataType::Int(None)),
        ConcreteDataType::UInt32(_) => Ok(SqlDataType::UnsignedInt(None)),
        ConcreteDataType::Int16(_) => Ok(SqlDataType::SmallInt(None)),
        ConcreteDataType::UInt16(_) => Ok(SqlDataType::UnsignedSmallInt(None)),
        ConcreteDataType::Int8(_) => Ok(SqlDataType::TinyInt(None)),
        ConcreteDataType::UInt8(_) => Ok(SqlDataType::UnsignedTinyInt(None)),
        ConcreteDataType::String(_) => Ok(SqlDataType::String(None)),
        ConcreteDataType::Float32(_) => Ok(SqlDataType::Float(None)),
        ConcreteDataType::Float64(_) => Ok(SqlDataType::Double),
        ConcreteDataType::Boolean(_) => Ok(SqlDataType::Boolean),
        ConcreteDataType::Date(_) => Ok(SqlDataType::Date),
        ConcreteDataType::DateTime(_) => Ok(SqlDataType::Datetime(None)),
        ConcreteDataType::Timestamp(ts_type) => Ok(SqlDataType::Timestamp(
            Some(ts_type.precision()),
            TimezoneInfo::None,
        )),
        ConcreteDataType::Time(time_type) => Ok(SqlDataType::Time(
            Some(time_type.precision()),
            TimezoneInfo::None,
        )),
        ConcreteDataType::Interval(_) => Ok(SqlDataType::Interval),
        ConcreteDataType::Binary(_) => Ok(SqlDataType::Varbinary(None)),
        ConcreteDataType::Decimal128(d) => Ok(SqlDataType::Decimal(
            ExactNumberInfo::PrecisionAndScale(d.precision() as u64, d.scale() as u64),
        )),
        ConcreteDataType::Duration(_)
        | ConcreteDataType::Null(_)
        | ConcreteDataType::List(_)
        | ConcreteDataType::Dictionary(_) => {
            unreachable!()
        }
    }
}

pub fn sql_location_to_grpc_add_column_location(
    location: &Option<AddColumnLocation>,
) -> Option<Location> {
    match location {
        Some(AddColumnLocation::First) => Some(Location {
            location_type: LocationType::First.into(),
            after_column_name: String::default(),
        }),
        Some(AddColumnLocation::After { column_name }) => Some(Location {
            location_type: LocationType::After.into(),
            after_column_name: column_name.to_string(),
        }),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::collections::HashMap;

    use api::v1::ColumnDataType;
    use common_time::timestamp::TimeUnit;
    use common_time::timezone::set_default_timezone;
    use datatypes::schema::FulltextAnalyzer;
    use datatypes::types::BooleanType;
    use datatypes::value::OrderedFloat;

    use super::*;
    use crate::ast::TimezoneInfo;
    use crate::statements::create::ColumnExtensions;
    use crate::statements::ColumnOption;
    use crate::{COLUMN_FULLTEXT_OPT_KEY_ANALYZER, COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE};

    fn check_type(sql_type: SqlDataType, data_type: ConcreteDataType) {
        assert_eq!(
            data_type,
            sql_data_type_to_concrete_data_type(&sql_type).unwrap()
        );
    }

    #[test]
    pub fn test_sql_data_type_to_concrete_data_type() {
        check_type(
            SqlDataType::BigInt(None),
            ConcreteDataType::int64_datatype(),
        );
        check_type(SqlDataType::Int(None), ConcreteDataType::int32_datatype());
        check_type(
            SqlDataType::Integer(None),
            ConcreteDataType::int32_datatype(),
        );
        check_type(
            SqlDataType::SmallInt(None),
            ConcreteDataType::int16_datatype(),
        );
        check_type(SqlDataType::Char(None), ConcreteDataType::string_datatype());
        check_type(
            SqlDataType::Varchar(None),
            ConcreteDataType::string_datatype(),
        );
        check_type(SqlDataType::Text, ConcreteDataType::string_datatype());
        check_type(
            SqlDataType::String(None),
            ConcreteDataType::string_datatype(),
        );
        check_type(
            SqlDataType::Float(None),
            ConcreteDataType::float32_datatype(),
        );
        check_type(SqlDataType::Double, ConcreteDataType::float64_datatype());
        check_type(SqlDataType::Boolean, ConcreteDataType::boolean_datatype());
        check_type(SqlDataType::Date, ConcreteDataType::date_datatype());
        check_type(
            SqlDataType::Timestamp(None, TimezoneInfo::None),
            ConcreteDataType::timestamp_millisecond_datatype(),
        );
        check_type(
            SqlDataType::Varbinary(None),
            ConcreteDataType::binary_datatype(),
        );
        check_type(
            SqlDataType::UnsignedBigInt(None),
            ConcreteDataType::uint64_datatype(),
        );
        check_type(
            SqlDataType::UnsignedInt(None),
            ConcreteDataType::uint32_datatype(),
        );
        check_type(
            SqlDataType::UnsignedSmallInt(None),
            ConcreteDataType::uint16_datatype(),
        );
        check_type(
            SqlDataType::UnsignedTinyInt(None),
            ConcreteDataType::uint8_datatype(),
        );
        check_type(
            SqlDataType::Datetime(None),
            ConcreteDataType::datetime_datatype(),
        );
        check_type(
            SqlDataType::Interval,
            ConcreteDataType::interval_month_day_nano_datatype(),
        );
    }

    #[test]
    fn test_sql_number_to_value() {
        let v = sql_number_to_value(&ConcreteDataType::float64_datatype(), "3.0").unwrap();
        assert_eq!(Value::Float64(OrderedFloat(3.0)), v);

        let v = sql_number_to_value(&ConcreteDataType::int32_datatype(), "999").unwrap();
        assert_eq!(Value::Int32(999), v);

        let v = sql_number_to_value(
            &ConcreteDataType::timestamp_nanosecond_datatype(),
            "1073741821",
        )
        .unwrap();
        assert_eq!(Value::Timestamp(Timestamp::new_nanosecond(1073741821)), v);

        let v = sql_number_to_value(
            &ConcreteDataType::timestamp_millisecond_datatype(),
            "999999",
        )
        .unwrap();
        assert_eq!(Value::Timestamp(Timestamp::new_millisecond(999999)), v);

        let v = sql_number_to_value(&ConcreteDataType::string_datatype(), "999");
        assert!(v.is_err(), "parse value error is: {v:?}");

        let v = sql_number_to_value(&ConcreteDataType::boolean_datatype(), "0").unwrap();
        assert_eq!(v, Value::Boolean(false));
        let v = sql_number_to_value(&ConcreteDataType::boolean_datatype(), "1").unwrap();
        assert_eq!(v, Value::Boolean(true));
        assert!(sql_number_to_value(&ConcreteDataType::boolean_datatype(), "2").is_err());
    }

    #[test]
    fn test_sql_value_to_value() {
        let sql_val = SqlValue::Null;
        assert_eq!(
            Value::Null,
            sql_value_to_value(
                "a",
                &ConcreteDataType::float64_datatype(),
                &sql_val,
                None,
                None
            )
            .unwrap()
        );

        let sql_val = SqlValue::Boolean(true);
        assert_eq!(
            Value::Boolean(true),
            sql_value_to_value(
                "a",
                &ConcreteDataType::boolean_datatype(),
                &sql_val,
                None,
                None
            )
            .unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        assert_eq!(
            Value::Float64(OrderedFloat(3.0)),
            sql_value_to_value(
                "a",
                &ConcreteDataType::float64_datatype(),
                &sql_val,
                None,
                None
            )
            .unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        let v = sql_value_to_value(
            "a",
            &ConcreteDataType::boolean_datatype(),
            &sql_val,
            None,
            None,
        );
        assert!(v.is_err());
        assert!(format!("{v:?}").contains("Failed to parse number '3.0' to boolean column type"));

        let sql_val = SqlValue::Boolean(true);
        let v = sql_value_to_value(
            "a",
            &ConcreteDataType::float64_datatype(),
            &sql_val,
            None,
            None,
        );
        assert!(v.is_err());
        assert!(
            format!("{v:?}").contains(
                "Column a expect type: Float64(Float64Type), actual: Boolean(BooleanType))"
            ),
            "v is {v:?}",
        );

        let sql_val = SqlValue::HexStringLiteral("48656c6c6f20776f726c6421".to_string());
        let v = sql_value_to_value(
            "a",
            &ConcreteDataType::binary_datatype(),
            &sql_val,
            None,
            None,
        )
        .unwrap();
        assert_eq!(Value::Binary(Bytes::from(b"Hello world!".as_slice())), v);

        let sql_val = SqlValue::DoubleQuotedString("MorningMyFriends".to_string());
        let v = sql_value_to_value(
            "a",
            &ConcreteDataType::binary_datatype(),
            &sql_val,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            Value::Binary(Bytes::from(b"MorningMyFriends".as_slice())),
            v
        );

        let sql_val = SqlValue::HexStringLiteral("9AF".to_string());
        let v = sql_value_to_value(
            "a",
            &ConcreteDataType::binary_datatype(),
            &sql_val,
            None,
            None,
        );
        assert!(v.is_err());
        assert!(
            format!("{v:?}").contains("odd number of digits"),
            "v is {v:?}"
        );

        let sql_val = SqlValue::HexStringLiteral("AG".to_string());
        let v = sql_value_to_value(
            "a",
            &ConcreteDataType::binary_datatype(),
            &sql_val,
            None,
            None,
        );
        assert!(v.is_err());
        assert!(format!("{v:?}").contains("invalid character"), "v is {v:?}",);
    }

    #[test]
    pub fn test_parse_date_literal() {
        let value = sql_value_to_value(
            "date",
            &ConcreteDataType::date_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22".to_string()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(ConcreteDataType::date_datatype(), value.data_type());
        if let Value::Date(d) = value {
            assert_eq!("2022-02-22", d.to_string());
        } else {
            unreachable!()
        }

        // with timezone
        let value = sql_value_to_value(
            "date",
            &ConcreteDataType::date_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22".to_string()),
            Some(&Timezone::from_tz_string("+07:00").unwrap()),
            None,
        )
        .unwrap();
        assert_eq!(ConcreteDataType::date_datatype(), value.data_type());
        if let Value::Date(d) = value {
            assert_eq!("2022-02-21", d.to_string());
        } else {
            unreachable!()
        }
    }

    #[test]
    pub fn test_parse_datetime_literal() {
        set_default_timezone(Some("Asia/Shanghai")).unwrap();
        let value = sql_value_to_value(
            "datetime_col",
            &ConcreteDataType::datetime_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22 00:01:03+0800".to_string()),
            None,
            None,
        )
        .unwrap();
        assert_eq!(ConcreteDataType::datetime_datatype(), value.data_type());
        if let Value::DateTime(d) = value {
            assert_eq!("2022-02-22 00:01:03+0800", d.to_string());
        } else {
            unreachable!()
        }
    }

    #[test]
    pub fn test_parse_illegal_datetime_literal() {
        assert!(sql_value_to_value(
            "datetime_col",
            &ConcreteDataType::datetime_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22 00:01:61".to_string()),
            None,
            None
        )
        .is_err());
    }

    #[test]
    fn test_parse_timestamp_literal() {
        match parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            &ConcreteDataType::timestamp_millisecond_datatype(),
            None,
        )
        .unwrap()
        {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000, ts.value());
                assert_eq!(TimeUnit::Millisecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        match parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            &ConcreteDataType::timestamp_datatype(TimeUnit::Second),
            None,
        )
        .unwrap()
        {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261, ts.value());
                assert_eq!(TimeUnit::Second, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        match parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            &ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond),
            None,
        )
        .unwrap()
        {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000000, ts.value());
                assert_eq!(TimeUnit::Microsecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        match parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            &ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond),
            None,
        )
        .unwrap()
        {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000000000, ts.value());
                assert_eq!(TimeUnit::Nanosecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        assert!(parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01+08".to_string(),
            &ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond),
            None,
        )
        .is_err());

        // with timezone
        match parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01".to_string(),
            &ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond),
            Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap()),
        )
        .unwrap()
        {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000000000, ts.value());
                assert_eq!("2022-02-21 16:01:01+0000", ts.to_iso8601_string());
                assert_eq!(TimeUnit::Nanosecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }
    }

    #[test]
    pub fn test_parse_column_default_constraint() {
        let bool_value = sqlparser::ast::Value::Boolean(true);

        let opts = vec![
            ColumnOptionDef {
                name: None,
                option: ColumnOption::Default(Expr::Value(bool_value)),
            },
            ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            },
        ];

        let constraint = parse_column_default_constraint(
            "coll",
            &ConcreteDataType::Boolean(BooleanType),
            &opts,
            None,
        )
        .unwrap();

        assert_matches!(
            constraint,
            Some(ColumnDefaultConstraint::Value(Value::Boolean(true)))
        );

        // Test negative number
        let opts = vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::Default(Expr::UnaryOp {
                op: UnaryOperator::Minus,
                expr: Box::new(Expr::Value(SqlValue::Number("32768".to_string(), false))),
            }),
        }];

        let constraint = parse_column_default_constraint(
            "coll",
            &ConcreteDataType::int16_datatype(),
            &opts,
            None,
        )
        .unwrap();

        assert_matches!(
            constraint,
            Some(ColumnDefaultConstraint::Value(Value::Int16(-32768)))
        );
    }

    #[test]
    fn test_incorrect_default_value_issue_3479() {
        let opts = vec![ColumnOptionDef {
            name: None,
            option: ColumnOption::Default(Expr::Value(SqlValue::Number(
                "0.047318541668048164".into(),
                false,
            ))),
        }];
        let constraint = parse_column_default_constraint(
            "coll",
            &ConcreteDataType::float64_datatype(),
            &opts,
            None,
        )
        .unwrap()
        .unwrap();
        assert_eq!("0.047318541668048164", constraint.to_string());
        let encoded: Vec<u8> = constraint.clone().try_into().unwrap();
        let decoded = ColumnDefaultConstraint::try_from(encoded.as_ref()).unwrap();
        assert_eq!(decoded, constraint);
    }

    #[test]
    pub fn test_sql_column_def_to_grpc_column_def() {
        // test basic
        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double,
            collation: None,
            options: vec![],
        };

        let grpc_column_def = sql_column_def_to_grpc_column_def(&column_def, None).unwrap();

        assert_eq!("col", grpc_column_def.name);
        assert!(grpc_column_def.is_nullable); // nullable when options are empty
        assert_eq!(ColumnDataType::Float64 as i32, grpc_column_def.data_type);
        assert!(grpc_column_def.default_constraint.is_empty());
        assert_eq!(grpc_column_def.semantic_type, SemanticType::Field as i32);

        // test not null
        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::NotNull,
            }],
        };

        let grpc_column_def = sql_column_def_to_grpc_column_def(&column_def, None).unwrap();
        assert!(!grpc_column_def.is_nullable);

        // test primary key
        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Unique {
                    is_primary: true,
                    characteristics: None,
                },
            }],
        };

        let grpc_column_def = sql_column_def_to_grpc_column_def(&column_def, None).unwrap();
        assert_eq!(grpc_column_def.semantic_type, SemanticType::Tag as i32);
    }

    #[test]
    pub fn test_sql_column_def_to_grpc_column_def_with_timezone() {
        let column_def = ColumnDef {
            name: "col".into(),
            // MILLISECOND
            data_type: SqlDataType::Timestamp(Some(3), TimezoneInfo::None),
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Default(Expr::Value(SqlValue::SingleQuotedString(
                    "2024-01-30T00:01:01".to_string(),
                ))),
            }],
        };

        // with timezone "Asia/Shanghai"
        let grpc_column_def = sql_column_def_to_grpc_column_def(
            &column_def,
            Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap()),
        )
        .unwrap();
        assert_eq!("col", grpc_column_def.name);
        assert!(grpc_column_def.is_nullable); // nullable when options are empty
        assert_eq!(
            ColumnDataType::TimestampMillisecond as i32,
            grpc_column_def.data_type
        );
        assert!(!grpc_column_def.default_constraint.is_empty());

        let constraint =
            ColumnDefaultConstraint::try_from(&grpc_column_def.default_constraint[..]).unwrap();
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-29 16:01:01+0000")
        );

        // without timezone
        let grpc_column_def = sql_column_def_to_grpc_column_def(&column_def, None).unwrap();
        assert_eq!("col", grpc_column_def.name);
        assert!(grpc_column_def.is_nullable); // nullable when options are empty
        assert_eq!(
            ColumnDataType::TimestampMillisecond as i32,
            grpc_column_def.data_type
        );
        assert!(!grpc_column_def.default_constraint.is_empty());

        let constraint =
            ColumnDefaultConstraint::try_from(&grpc_column_def.default_constraint[..]).unwrap();
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-30 00:01:01+0000")
        );
    }

    #[test]
    pub fn test_has_primary_key_option() {
        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double,
            collation: None,
            options: vec![],
        };
        assert!(!has_primary_key_option(&column_def));

        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double,
            collation: None,
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Unique {
                    is_primary: true,
                    characteristics: None,
                },
            }],
        };
        assert!(has_primary_key_option(&column_def));
    }

    #[test]
    pub fn test_column_to_schema() {
        let column_def = Column {
            column_def: ColumnDef {
                name: "col".into(),
                data_type: SqlDataType::Double,
                collation: None,
                options: vec![],
            },
            extensions: ColumnExtensions::default(),
        };

        let column_schema = column_to_schema(&column_def, false, None).unwrap();

        assert_eq!("col", column_schema.name);
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            column_schema.data_type
        );
        assert!(column_schema.is_nullable());
        assert!(!column_schema.is_time_index());

        let column_schema = column_to_schema(&column_def, true, None).unwrap();

        assert_eq!("col", column_schema.name);
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            column_schema.data_type
        );
        assert!(!column_schema.is_nullable());
        assert!(column_schema.is_time_index());

        let column_def = Column {
            column_def: ColumnDef {
                name: "col2".into(),
                data_type: SqlDataType::String(None),
                collation: None,
                options: vec![
                    ColumnOptionDef {
                        name: None,
                        option: ColumnOption::NotNull,
                    },
                    ColumnOptionDef {
                        name: None,
                        option: ColumnOption::Comment("test comment".to_string()),
                    },
                ],
            },
            extensions: ColumnExtensions::default(),
        };

        let column_schema = column_to_schema(&column_def, false, None).unwrap();

        assert_eq!("col2", column_schema.name);
        assert_eq!(ConcreteDataType::string_datatype(), column_schema.data_type);
        assert!(!column_schema.is_nullable());
        assert!(!column_schema.is_time_index());
        assert_eq!(
            column_schema.metadata().get(COMMENT_KEY),
            Some(&"test comment".to_string())
        );
    }

    #[test]
    pub fn test_column_to_schema_timestamp_with_timezone() {
        let column = Column {
            column_def: ColumnDef {
                name: "col".into(),
                // MILLISECOND
                data_type: SqlDataType::Timestamp(Some(3), TimezoneInfo::None),
                collation: None,
                options: vec![ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Default(Expr::Value(SqlValue::SingleQuotedString(
                        "2024-01-30T00:01:01".to_string(),
                    ))),
                }],
            },
            extensions: ColumnExtensions::default(),
        };

        // with timezone "Asia/Shanghai"

        let column_schema = column_to_schema(
            &column,
            false,
            Some(&Timezone::from_tz_string("Asia/Shanghai").unwrap()),
        )
        .unwrap();

        assert_eq!("col", column_schema.name);
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            column_schema.data_type
        );
        assert!(column_schema.is_nullable());

        let constraint = column_schema.default_constraint().unwrap();
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-29 16:01:01+0000")
        );

        // without timezone
        let column_schema = column_to_schema(&column, false, None).unwrap();

        assert_eq!("col", column_schema.name);
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            column_schema.data_type
        );
        assert!(column_schema.is_nullable());

        let constraint = column_schema.default_constraint().unwrap();
        assert!(
            matches!(constraint, ColumnDefaultConstraint::Value(Value::Timestamp(ts))
                         if ts.to_iso8601_string() == "2024-01-30 00:01:01+0000")
        );
    }

    #[test]
    fn test_column_to_schema_with_fulltext() {
        let column = Column {
            column_def: ColumnDef {
                name: "col".into(),
                data_type: SqlDataType::Text,
                collation: None,
                options: vec![],
            },
            extensions: ColumnExtensions {
                fulltext_options: Some(
                    HashMap::from_iter([
                        (
                            COLUMN_FULLTEXT_OPT_KEY_ANALYZER.to_string(),
                            "English".to_string(),
                        ),
                        (
                            COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE.to_string(),
                            "true".to_string(),
                        ),
                    ])
                    .into(),
                ),
            },
        };

        let column_schema = column_to_schema(&column, false, None).unwrap();
        assert_eq!("col", column_schema.name);
        assert_eq!(ConcreteDataType::string_datatype(), column_schema.data_type);
        let fulltext_options = column_schema.fulltext_options().unwrap().unwrap();
        assert_eq!(fulltext_options.analyzer, FulltextAnalyzer::English);
        assert!(fulltext_options.case_sensitive);
    }

    #[test]
    pub fn test_parse_placeholder_value() {
        assert!(sql_value_to_value(
            "test",
            &ConcreteDataType::string_datatype(),
            &SqlValue::Placeholder("default".into()),
            None,
            None
        )
        .is_err());
        assert!(sql_value_to_value(
            "test",
            &ConcreteDataType::string_datatype(),
            &SqlValue::Placeholder("default".into()),
            None,
            Some(UnaryOperator::Minus),
        )
        .is_err());
        assert!(sql_value_to_value(
            "test",
            &ConcreteDataType::uint16_datatype(),
            &SqlValue::Number("3".into(), false),
            None,
            Some(UnaryOperator::Minus),
        )
        .is_err());
        assert!(sql_value_to_value(
            "test",
            &ConcreteDataType::uint16_datatype(),
            &SqlValue::Number("3".into(), false),
            None,
            None
        )
        .is_ok());
    }
}
