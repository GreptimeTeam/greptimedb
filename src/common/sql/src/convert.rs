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

use std::str::FromStr;

use arrow_schema::extension::ExtensionType;
use common_time::Timestamp;
use common_time::timezone::Timezone;
use datatypes::extension::json::JsonExtensionType;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use datatypes::types::{JsonFormat, parse_string_to_jsonb, parse_string_to_vector_type_value};
use datatypes::value::{OrderedF32, OrderedF64, Value};
use snafu::{OptionExt, ResultExt, ensure};
pub use sqlparser::ast::{
    BinaryOperator, ColumnDef, ColumnOption, ColumnOptionDef, DataType, Expr, Function,
    FunctionArg, FunctionArgExpr, FunctionArguments, Ident, ObjectName, SqlOption, TableConstraint,
    TimezoneInfo, UnaryOperator, Value as SqlValue, Visit, VisitMut, Visitor, VisitorMut,
    visit_expressions_mut, visit_statements_mut,
};

use crate::error::{
    ColumnTypeMismatchSnafu, ConvertSqlValueSnafu, ConvertStrSnafu, DatatypeSnafu,
    DeserializeSnafu, InvalidCastSnafu, InvalidSqlValueSnafu, InvalidUnaryOpSnafu,
    ParseSqlValueSnafu, Result, TimestampOverflowSnafu, UnsupportedUnaryOpSnafu,
};

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
                let n = parse_sql_number::<i64>($n)?;
                let timestamp = Timestamp::new(n, t.unit());

                // Check if the value is within the valid range for the target unit
                if Timestamp::is_overflow(n, t.unit()) {
                    return TimestampOverflowSnafu {
                        timestamp,
                        target_unit: t.unit(),
                    }.fail();
                }

                Ok(Value::Timestamp(timestamp))
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
pub(crate) fn sql_number_to_value(data_type: &ConcreteDataType, n: &str) -> Result<Value> {
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

/// Converts SQL value to value according to the data type.
/// If `auto_string_to_numeric` is true, tries to cast the string value to numeric values,
/// and returns error if the cast fails.
pub fn sql_value_to_value(
    column_schema: &ColumnSchema,
    sql_val: &SqlValue,
    timezone: Option<&Timezone>,
    unary_op: Option<UnaryOperator>,
    auto_string_to_numeric: bool,
) -> Result<Value> {
    let column_name = &column_schema.name;
    let data_type = &column_schema.data_type;
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
            parse_string_to_value(column_schema, s.clone(), timezone, auto_string_to_numeric)?
        }
        SqlValue::HexStringLiteral(s) => {
            // Should not directly write binary into json column
            ensure!(
                !matches!(data_type, ConcreteDataType::Json(_)),
                ColumnTypeMismatchSnafu {
                    column_name,
                    expect: ConcreteDataType::binary_datatype(),
                    actual: ConcreteDataType::json_datatype(),
                }
            );

            parse_hex_string(s)?
        }
        SqlValue::Placeholder(s) => return InvalidSqlValueSnafu { value: s }.fail(),

        // TODO(dennis): supports binary string
        _ => {
            return ConvertSqlValueSnafu {
                value: sql_val.clone(),
                datatype: data_type.clone(),
            }
            .fail();
        }
    };

    if let Some(unary_op) = unary_op {
        match unary_op {
            UnaryOperator::Plus | UnaryOperator::Minus | UnaryOperator::Not => {}
            _ => {
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
            | Value::Timestamp(_)
            | Value::Time(_)
            | Value::Duration(_)
            | Value::IntervalYearMonth(_)
            | Value::IntervalDayTime(_)
            | Value::IntervalMonthDayNano(_) => match unary_op {
                UnaryOperator::Plus => {}
                UnaryOperator::Minus => {
                    value = value
                        .try_negative()
                        .with_context(|| InvalidUnaryOpSnafu { unary_op, value })?;
                }
                _ => return InvalidUnaryOpSnafu { unary_op, value }.fail(),
            },

            Value::String(_)
            | Value::Binary(_)
            | Value::List(_)
            | Value::Struct(_)
            | Value::Json(_) => {
                return InvalidUnaryOpSnafu { unary_op, value }.fail();
            }
        }
    }

    let value_datatype = value.data_type();
    // The datatype of json value is determined by its actual data, so we can't simply "cast" it here.
    if value_datatype.is_json() || value_datatype == *data_type {
        Ok(value)
    } else {
        datatypes::types::cast(value, data_type).with_context(|_| InvalidCastSnafu {
            sql_value: sql_val.clone(),
            datatype: data_type,
        })
    }
}

pub(crate) fn parse_string_to_value(
    column_schema: &ColumnSchema,
    s: String,
    timezone: Option<&Timezone>,
    auto_string_to_numeric: bool,
) -> Result<Value> {
    let data_type = &column_schema.data_type;
    if auto_string_to_numeric && let Some(value) = auto_cast_to_numeric(&s, data_type)? {
        return Ok(value);
    }

    ensure!(
        data_type.is_stringifiable(),
        ColumnTypeMismatchSnafu {
            column_name: column_schema.name.clone(),
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
        ConcreteDataType::Timestamp(t) => {
            if let Ok(ts) = Timestamp::from_str(&s, timezone) {
                Ok(Value::Timestamp(ts.convert_to(t.unit()).context(
                    TimestampOverflowSnafu {
                        timestamp: ts,
                        target_unit: t.unit(),
                    },
                )?))
            } else if let Ok(ts) = i64::from_str(s.as_str()) {
                Ok(Value::Timestamp(Timestamp::new(ts, t.unit())))
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
        ConcreteDataType::Json(j) => match &j.format {
            JsonFormat::Jsonb => {
                let v = parse_string_to_jsonb(&s).context(DatatypeSnafu)?;
                Ok(Value::Binary(v.into()))
            }
            JsonFormat::Native(_) => {
                let extension_type: Option<JsonExtensionType> =
                    column_schema.extension_type().context(DatatypeSnafu)?;
                let json_structure_settings = extension_type
                    .and_then(|x| x.metadata().json_structure_settings.clone())
                    .unwrap_or_default();
                let v = serde_json::from_str(&s).context(DeserializeSnafu { json: s })?;
                json_structure_settings.encode(v).context(DatatypeSnafu)
            }
        },
        ConcreteDataType::Vector(d) => {
            let v = parse_string_to_vector_type_value(&s, Some(d.dim)).context(DatatypeSnafu)?;
            Ok(Value::Binary(v.into()))
        }
        _ => ParseSqlValueSnafu {
            msg: format!("Failed to parse {s} to {data_type} value"),
        }
        .fail(),
    }
}

/// Casts string to value of specified numeric data type.
/// If the string cannot be parsed, returns an error.
///
/// Returns None if the data type doesn't support auto casting.
pub(crate) fn auto_cast_to_numeric(s: &str, data_type: &ConcreteDataType) -> Result<Option<Value>> {
    let value = match data_type {
        ConcreteDataType::Boolean(_) => s.parse::<bool>().map(Value::Boolean).ok(),
        ConcreteDataType::Int8(_) => s.parse::<i8>().map(Value::Int8).ok(),
        ConcreteDataType::Int16(_) => s.parse::<i16>().map(Value::Int16).ok(),
        ConcreteDataType::Int32(_) => s.parse::<i32>().map(Value::Int32).ok(),
        ConcreteDataType::Int64(_) => s.parse::<i64>().map(Value::Int64).ok(),
        ConcreteDataType::UInt8(_) => s.parse::<u8>().map(Value::UInt8).ok(),
        ConcreteDataType::UInt16(_) => s.parse::<u16>().map(Value::UInt16).ok(),
        ConcreteDataType::UInt32(_) => s.parse::<u32>().map(Value::UInt32).ok(),
        ConcreteDataType::UInt64(_) => s.parse::<u64>().map(Value::UInt64).ok(),
        ConcreteDataType::Float32(_) => s
            .parse::<f32>()
            .map(|v| Value::Float32(OrderedF32::from(v)))
            .ok(),
        ConcreteDataType::Float64(_) => s
            .parse::<f64>()
            .map(|v| Value::Float64(OrderedF64::from(v)))
            .ok(),
        _ => return Ok(None),
    };

    match value {
        Some(value) => Ok(Some(value)),
        None => ConvertStrSnafu {
            value: s,
            datatype: data_type.clone(),
        }
        .fail(),
    }
}

pub(crate) fn parse_hex_string(s: &str) -> Result<Value> {
    match hex::decode(s) {
        Ok(b) => Ok(Value::Binary(common_base::bytes::Bytes::from(b))),
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

/// Deserialize default constraint from json bytes
pub fn deserialize_default_constraint(
    bytes: &[u8],
    column_name: &str,
    data_type: &ConcreteDataType,
) -> Result<Option<ColumnDefaultConstraint>> {
    let json = String::from_utf8_lossy(bytes);
    let default_constraint = serde_json::from_str(&json).context(DeserializeSnafu { json })?;
    let column_def = sqlparser::ast::ColumnOptionDef {
        name: None,
        option: sqlparser::ast::ColumnOption::Default(default_constraint),
    };

    crate::default_constraint::parse_column_default_constraint(
        column_name,
        data_type,
        &[column_def],
        None,
    )
}

#[cfg(test)]
mod test {
    use common_base::bytes::Bytes;
    use common_time::timestamp::TimeUnit;
    use datatypes::types::TimestampType;
    use datatypes::value::OrderedFloat;

    use super::*;

    macro_rules! call_parse_string_to_value {
        ($column_name: expr, $input: expr, $data_type: expr) => {
            call_parse_string_to_value!($column_name, $input, $data_type, None)
        };
        ($column_name: expr, $input: expr, $data_type: expr, timezone = $timezone: expr) => {
            call_parse_string_to_value!($column_name, $input, $data_type, Some($timezone))
        };
        ($column_name: expr, $input: expr, $data_type: expr, $timezone: expr) => {{
            let column_schema = ColumnSchema::new($column_name, $data_type, true);
            parse_string_to_value(&column_schema, $input, $timezone, true)
        }};
    }

    #[test]
    fn test_string_to_value_auto_numeric() -> Result<()> {
        // Test string to boolean with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "true".to_string(),
            ConcreteDataType::boolean_datatype()
        )?;
        assert_eq!(Value::Boolean(true), result);

        // Test invalid string to boolean with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_boolean".to_string(),
            ConcreteDataType::boolean_datatype()
        );
        assert!(result.is_err());

        // Test string to int8
        let result = call_parse_string_to_value!(
            "col",
            "42".to_string(),
            ConcreteDataType::int8_datatype()
        )?;
        assert_eq!(Value::Int8(42), result);

        // Test invalid string to int8 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_an_int8".to_string(),
            ConcreteDataType::int8_datatype()
        );
        assert!(result.is_err());

        // Test string to int16
        let result = call_parse_string_to_value!(
            "col",
            "1000".to_string(),
            ConcreteDataType::int16_datatype()
        )?;
        assert_eq!(Value::Int16(1000), result);

        // Test invalid string to int16 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_an_int16".to_string(),
            ConcreteDataType::int16_datatype()
        );
        assert!(result.is_err());

        // Test string to int32
        let result = call_parse_string_to_value!(
            "col",
            "100000".to_string(),
            ConcreteDataType::int32_datatype()
        )?;
        assert_eq!(Value::Int32(100000), result);

        // Test invalid string to int32 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_an_int32".to_string(),
            ConcreteDataType::int32_datatype()
        );
        assert!(result.is_err());

        // Test string to int64
        let result = call_parse_string_to_value!(
            "col",
            "1000000".to_string(),
            ConcreteDataType::int64_datatype()
        )?;
        assert_eq!(Value::Int64(1000000), result);

        // Test invalid string to int64 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_an_int64".to_string(),
            ConcreteDataType::int64_datatype()
        );
        assert!(result.is_err());

        // Test string to uint8
        let result = call_parse_string_to_value!(
            "col",
            "200".to_string(),
            ConcreteDataType::uint8_datatype()
        )?;
        assert_eq!(Value::UInt8(200), result);

        // Test invalid string to uint8 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_uint8".to_string(),
            ConcreteDataType::uint8_datatype()
        );
        assert!(result.is_err());

        // Test string to uint16
        let result = call_parse_string_to_value!(
            "col",
            "60000".to_string(),
            ConcreteDataType::uint16_datatype()
        )?;
        assert_eq!(Value::UInt16(60000), result);

        // Test invalid string to uint16 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_uint16".to_string(),
            ConcreteDataType::uint16_datatype()
        );
        assert!(result.is_err());

        // Test string to uint32
        let result = call_parse_string_to_value!(
            "col",
            "4000000000".to_string(),
            ConcreteDataType::uint32_datatype()
        )?;
        assert_eq!(Value::UInt32(4000000000), result);

        // Test invalid string to uint32 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_uint32".to_string(),
            ConcreteDataType::uint32_datatype()
        );
        assert!(result.is_err());

        // Test string to uint64
        let result = call_parse_string_to_value!(
            "col",
            "18446744073709551615".to_string(),
            ConcreteDataType::uint64_datatype()
        )?;
        assert_eq!(Value::UInt64(18446744073709551615), result);

        // Test invalid string to uint64 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_uint64".to_string(),
            ConcreteDataType::uint64_datatype()
        );
        assert!(result.is_err());

        // Test string to float32
        let result = call_parse_string_to_value!(
            "col",
            "3.5".to_string(),
            ConcreteDataType::float32_datatype()
        )?;
        assert_eq!(Value::Float32(OrderedF32::from(3.5)), result);

        // Test invalid string to float32 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_float32".to_string(),
            ConcreteDataType::float32_datatype()
        );
        assert!(result.is_err());

        // Test string to float64
        let result = call_parse_string_to_value!(
            "col",
            "3.5".to_string(),
            ConcreteDataType::float64_datatype()
        )?;
        assert_eq!(Value::Float64(OrderedF64::from(3.5)), result);

        // Test invalid string to float64 with auto cast
        let result = call_parse_string_to_value!(
            "col",
            "not_a_float64".to_string(),
            ConcreteDataType::float64_datatype()
        );
        assert!(result.is_err());
        Ok(())
    }

    macro_rules! call_sql_value_to_value {
        ($column_name: expr, $data_type: expr, $sql_value: expr) => {
            call_sql_value_to_value!($column_name, $data_type, $sql_value, None, None, false)
        };
        ($column_name: expr, $data_type: expr, $sql_value: expr, timezone = $timezone: expr) => {
            call_sql_value_to_value!(
                $column_name,
                $data_type,
                $sql_value,
                Some($timezone),
                None,
                false
            )
        };
        ($column_name: expr, $data_type: expr, $sql_value: expr, unary_op = $unary_op: expr) => {
            call_sql_value_to_value!(
                $column_name,
                $data_type,
                $sql_value,
                None,
                Some($unary_op),
                false
            )
        };
        ($column_name: expr, $data_type: expr, $sql_value: expr, auto_string_to_numeric) => {
            call_sql_value_to_value!($column_name, $data_type, $sql_value, None, None, true)
        };
        ($column_name: expr, $data_type: expr, $sql_value: expr, $timezone: expr, $unary_op: expr, $auto_string_to_numeric: expr) => {{
            let column_schema = ColumnSchema::new($column_name, $data_type, true);
            sql_value_to_value(
                &column_schema,
                $sql_value,
                $timezone,
                $unary_op,
                $auto_string_to_numeric,
            )
        }};
    }

    #[test]
    fn test_sql_value_to_value() -> Result<()> {
        let sql_val = SqlValue::Null;
        assert_eq!(
            Value::Null,
            call_sql_value_to_value!("a", ConcreteDataType::float64_datatype(), &sql_val)?
        );

        let sql_val = SqlValue::Boolean(true);
        assert_eq!(
            Value::Boolean(true),
            call_sql_value_to_value!("a", ConcreteDataType::boolean_datatype(), &sql_val)?
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        assert_eq!(
            Value::Float64(OrderedFloat(3.0)),
            call_sql_value_to_value!("a", ConcreteDataType::float64_datatype(), &sql_val)?
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        let v = call_sql_value_to_value!("a", ConcreteDataType::boolean_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{v:?}").contains("Failed to parse number '3.0' to boolean column type"));

        let sql_val = SqlValue::Boolean(true);
        let v = call_sql_value_to_value!("a", ConcreteDataType::float64_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(
            format!("{v:?}").contains(
                "Column a expect type: Float64(Float64Type), actual: Boolean(BooleanType)"
            ),
            "v is {v:?}",
        );

        let sql_val = SqlValue::HexStringLiteral("48656c6c6f20776f726c6421".to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::binary_datatype(), &sql_val)?;
        assert_eq!(Value::Binary(Bytes::from(b"Hello world!".as_slice())), v);

        let sql_val = SqlValue::DoubleQuotedString("MorningMyFriends".to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::binary_datatype(), &sql_val)?;
        assert_eq!(
            Value::Binary(Bytes::from(b"MorningMyFriends".as_slice())),
            v
        );

        let sql_val = SqlValue::HexStringLiteral("9AF".to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::binary_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(
            format!("{v:?}").contains("odd number of digits"),
            "v is {v:?}"
        );

        let sql_val = SqlValue::HexStringLiteral("AG".to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::binary_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{v:?}").contains("invalid character"), "v is {v:?}",);

        let sql_val = SqlValue::DoubleQuotedString("MorningMyFriends".to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::json_datatype(), &sql_val);
        assert!(v.is_err());

        let sql_val = SqlValue::DoubleQuotedString(r#"{"a":"b"}"#.to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::json_datatype(), &sql_val)?;
        assert_eq!(
            Value::Binary(Bytes::from(
                jsonb::parse_value(r#"{"a":"b"}"#.as_bytes())
                    .unwrap()
                    .to_vec()
                    .as_slice()
            )),
            v
        );
        Ok(())
    }

    #[test]
    fn test_parse_json_to_jsonb() {
        match call_parse_string_to_value!(
            "json_col",
            r#"{"a": "b"}"#.to_string(),
            ConcreteDataType::json_datatype()
        ) {
            Ok(Value::Binary(b)) => {
                assert_eq!(
                    b,
                    jsonb::parse_value(r#"{"a": "b"}"#.as_bytes())
                        .unwrap()
                        .to_vec()
                );
            }
            _ => {
                unreachable!()
            }
        }

        assert!(
            call_parse_string_to_value!(
                "json_col",
                r#"Nicola Kovac is the best rifler in the world"#.to_string(),
                ConcreteDataType::json_datatype()
            )
            .is_err()
        )
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
    fn test_parse_date_literal() {
        let value = call_sql_value_to_value!(
            "date",
            ConcreteDataType::date_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22".to_string())
        )
        .unwrap();
        assert_eq!(ConcreteDataType::date_datatype(), value.data_type());
        if let Value::Date(d) = value {
            assert_eq!("2022-02-22", d.to_string());
        } else {
            unreachable!()
        }

        // with timezone
        let value = call_sql_value_to_value!(
            "date",
            ConcreteDataType::date_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22".to_string()),
            timezone = &Timezone::from_tz_string("+07:00").unwrap()
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
    fn test_parse_timestamp_literal() -> Result<()> {
        match call_parse_string_to_value!(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            ConcreteDataType::timestamp_millisecond_datatype()
        )? {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000, ts.value());
                assert_eq!(TimeUnit::Millisecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        match call_parse_string_to_value!(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            ConcreteDataType::timestamp_datatype(TimeUnit::Second)
        )? {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261, ts.value());
                assert_eq!(TimeUnit::Second, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        match call_parse_string_to_value!(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            ConcreteDataType::timestamp_datatype(TimeUnit::Microsecond)
        )? {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000000, ts.value());
                assert_eq!(TimeUnit::Microsecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        match call_parse_string_to_value!(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond)
        )? {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000000000, ts.value());
                assert_eq!(TimeUnit::Nanosecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }

        assert!(
            call_parse_string_to_value!(
                "timestamp_col",
                "2022-02-22T00:01:01+08".to_string(),
                ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond)
            )
            .is_err()
        );

        // with timezone
        match call_parse_string_to_value!(
            "timestamp_col",
            "2022-02-22T00:01:01".to_string(),
            ConcreteDataType::timestamp_datatype(TimeUnit::Nanosecond),
            timezone = &Timezone::from_tz_string("Asia/Shanghai").unwrap()
        )? {
            Value::Timestamp(ts) => {
                assert_eq!(1645459261000000000, ts.value());
                assert_eq!("2022-02-21 16:01:01+0000", ts.to_iso8601_string());
                assert_eq!(TimeUnit::Nanosecond, ts.unit());
            }
            _ => {
                unreachable!()
            }
        }
        Ok(())
    }

    #[test]
    fn test_parse_placeholder_value() {
        assert!(
            call_sql_value_to_value!(
                "test",
                ConcreteDataType::string_datatype(),
                &SqlValue::Placeholder("default".into())
            )
            .is_err()
        );
        assert!(
            call_sql_value_to_value!(
                "test",
                ConcreteDataType::string_datatype(),
                &SqlValue::Placeholder("default".into()),
                unary_op = UnaryOperator::Minus
            )
            .is_err()
        );
        assert!(
            call_sql_value_to_value!(
                "test",
                ConcreteDataType::uint16_datatype(),
                &SqlValue::Number("3".into(), false),
                unary_op = UnaryOperator::Minus
            )
            .is_err()
        );
        assert!(
            call_sql_value_to_value!(
                "test",
                ConcreteDataType::uint16_datatype(),
                &SqlValue::Number("3".into(), false)
            )
            .is_ok()
        );
    }

    #[test]
    fn test_auto_string_to_numeric() {
        // Test with auto_string_to_numeric=true
        let sql_val = SqlValue::SingleQuotedString("123".to_string());
        let v = call_sql_value_to_value!(
            "a",
            ConcreteDataType::int32_datatype(),
            &sql_val,
            auto_string_to_numeric
        )
        .unwrap();
        assert_eq!(Value::Int32(123), v);

        // Test with a float string
        let sql_val = SqlValue::SingleQuotedString("3.5".to_string());
        let v = call_sql_value_to_value!(
            "a",
            ConcreteDataType::float64_datatype(),
            &sql_val,
            auto_string_to_numeric
        )
        .unwrap();
        assert_eq!(Value::Float64(OrderedFloat(3.5)), v);

        // Test with auto_string_to_numeric=false
        let sql_val = SqlValue::SingleQuotedString("123".to_string());
        let v = call_sql_value_to_value!("a", ConcreteDataType::int32_datatype(), &sql_val);
        assert!(v.is_err());

        // Test with an invalid numeric string but auto_string_to_numeric=true
        // Should return an error now with the new auto_cast_to_numeric behavior
        let sql_val = SqlValue::SingleQuotedString("not_a_number".to_string());
        let v = call_sql_value_to_value!(
            "a",
            ConcreteDataType::int32_datatype(),
            &sql_val,
            auto_string_to_numeric
        );
        assert!(v.is_err());

        // Test with boolean type
        let sql_val = SqlValue::SingleQuotedString("true".to_string());
        let v = call_sql_value_to_value!(
            "a",
            ConcreteDataType::boolean_datatype(),
            &sql_val,
            auto_string_to_numeric
        )
        .unwrap();
        assert_eq!(Value::Boolean(true), v);

        // Non-numeric types should still be handled normally
        let sql_val = SqlValue::SingleQuotedString("hello".to_string());
        let v = call_sql_value_to_value!(
            "a",
            ConcreteDataType::string_datatype(),
            &sql_val,
            auto_string_to_numeric
        );
        assert!(v.is_ok());
    }

    #[test]
    fn test_sql_number_to_value_timestamp_strict_typing() {
        // Test that values are interpreted according to the target column type
        let timestamp_type = TimestampType::Millisecond(datatypes::types::TimestampMillisecondType);
        let data_type = ConcreteDataType::Timestamp(timestamp_type);

        // Valid millisecond timestamp
        let millisecond_str = "1747814093865";
        let result = sql_number_to_value(&data_type, millisecond_str).unwrap();
        if let Value::Timestamp(ts) = result {
            assert_eq!(ts.unit(), TimeUnit::Millisecond);
            assert_eq!(ts.value(), 1747814093865);
        } else {
            panic!("Expected timestamp value");
        }

        // Large value that would overflow when treated as milliseconds should be rejected
        let nanosecond_str = "1747814093865000000"; // This is too large for millisecond precision
        let result = sql_number_to_value(&data_type, nanosecond_str);
        assert!(
            result.is_err(),
            "Should reject overly large timestamp values"
        );
    }

    #[test]
    fn test_sql_number_to_value_timestamp_different_units() {
        // Test second precision
        let second_type = TimestampType::Second(datatypes::types::TimestampSecondType);
        let second_data_type = ConcreteDataType::Timestamp(second_type);

        let second_str = "1747814093";
        let result = sql_number_to_value(&second_data_type, second_str).unwrap();
        if let Value::Timestamp(ts) = result {
            assert_eq!(ts.unit(), TimeUnit::Second);
            assert_eq!(ts.value(), 1747814093);
        } else {
            panic!("Expected timestamp value");
        }

        // Test nanosecond precision
        let nanosecond_type = TimestampType::Nanosecond(datatypes::types::TimestampNanosecondType);
        let nanosecond_data_type = ConcreteDataType::Timestamp(nanosecond_type);

        let nanosecond_str = "1747814093865000000";
        let result = sql_number_to_value(&nanosecond_data_type, nanosecond_str).unwrap();
        if let Value::Timestamp(ts) = result {
            assert_eq!(ts.unit(), TimeUnit::Nanosecond);
            assert_eq!(ts.value(), 1747814093865000000);
        } else {
            panic!("Expected timestamp value");
        }
    }

    #[test]
    fn test_timestamp_range_validation() {
        // Test that our range checking works correctly
        let nanosecond_value = 1747814093865000000i64; // This should be too large for millisecond

        // This should work for nanosecond precision
        let nanosecond_type = TimestampType::Nanosecond(datatypes::types::TimestampNanosecondType);
        let nanosecond_data_type = ConcreteDataType::Timestamp(nanosecond_type);
        let result = sql_number_to_value(&nanosecond_data_type, "1747814093865000000");
        assert!(
            result.is_ok(),
            "Nanosecond value should be valid for nanosecond column"
        );

        // This should fail for millisecond precision (value too large)
        let millisecond_type =
            TimestampType::Millisecond(datatypes::types::TimestampMillisecondType);
        let millisecond_data_type = ConcreteDataType::Timestamp(millisecond_type);
        let result = sql_number_to_value(&millisecond_data_type, "1747814093865000000");
        assert!(
            result.is_err(),
            "Nanosecond value should be rejected for millisecond column"
        );

        // Verify the ranges work as expected
        assert!(
            nanosecond_value > Timestamp::MAX_MILLISECOND.value(),
            "Test value should exceed millisecond range"
        );
    }
}
