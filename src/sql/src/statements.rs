pub mod alter;
pub mod create;
pub mod insert;
pub mod query;
pub mod show;
pub mod statement;

use std::str::FromStr;

use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_time::Timestamp;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use datatypes::types::DateTimeType;
use datatypes::value::Value;
use snafu::{ensure, ResultExt};

use crate::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, DataType as SqlDataType, Expr, ObjectName,
    Value as SqlValue,
};
use crate::error::{
    self, ColumnTypeMismatchSnafu, ParseSqlValueSnafu, Result, UnsupportedDefaultValueSnafu,
};

/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>` or `<table>` when
/// catalog and schema are default) to tuple.
pub fn table_idents_to_full_name(obj_name: &ObjectName) -> Result<(String, String, String)> {
    match &obj_name.0[..] {
        [table] => Ok((
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
            table.value.clone(),
        )),
        [catalog, schema, table] => Ok((
            catalog.value.clone(),
            schema.value.clone(),
            table.value.clone(),
        )),
        _ => error::InvalidSqlSnafu {
            msg: format!(
                "expect table name to be <catalog>.<schema>.<table> or <table>, actual: {}",
                obj_name
            ),
        }
        .fail(),
    }
}

fn parse_string_to_value(
    column_name: &str,
    s: String,
    data_type: &ConcreteDataType,
) -> Result<Value> {
    ensure!(
        data_type.is_string(),
        ColumnTypeMismatchSnafu {
            column_name,
            expect: data_type.clone(),
            actual: ConcreteDataType::string_datatype(),
        }
    );

    match data_type {
        ConcreteDataType::String(_) => Ok(Value::String(s.into())),
        ConcreteDataType::Date(_) => {
            if let Ok(date) = common_time::date::Date::from_str(&s) {
                Ok(Value::Date(date))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Failed to parse {} to Date value", s),
                }
                .fail()
            }
        }
        ConcreteDataType::DateTime(_) => {
            if let Ok(datetime) = common_time::datetime::DateTime::from_str(&s) {
                Ok(Value::DateTime(datetime))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Failed to parse {} to DateTime value", s),
                }
                .fail()
            }
        }
        ConcreteDataType::Timestamp(t) => {
            if let Ok(ts) = Timestamp::from_str(&s) {
                Ok(Value::Timestamp(Timestamp::new(
                    ts.convert_to(t.unit),
                    t.unit,
                )))
            } else {
                ParseSqlValueSnafu {
                    msg: format!("Failed to parse {} to Timestamp value", s),
                }
                .fail()
            }
        }
        _ => {
            unreachable!()
        }
    }
}

macro_rules! parse_number_to_value {
    ($data_type: expr, $n: ident,  $(($Type: ident, $PrimitiveType: ident)), +) => {
        match $data_type {
            $(
                ConcreteDataType::$Type(_) => {
                    let n  = parse_sql_number::<$PrimitiveType>($n)?;
                    Ok(Value::from(n))
                },
            )+
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
        (UInt8, u8),
        (UInt16, u16),
        (UInt32, u32),
        (UInt64, u64),
        (Int8, i8),
        (Int16, i16),
        (Int32, i32),
        (Int64, i64),
        (Float64, f64),
        (Float32, f32),
        (Timestamp, i64)
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
            msg: format!("Fail to parse number {}, {:?}", n, e),
        }
        .fail(),
    }
}

pub fn sql_value_to_value(
    column_name: &str,
    data_type: &ConcreteDataType,
    sql_val: &SqlValue,
) -> Result<Value> {
    Ok(match sql_val {
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
            parse_string_to_value(column_name, s.to_owned(), data_type)?
        }
        _ => todo!("Other sql value"),
    })
}

fn parse_column_default_constraint(
    column_name: &str,
    data_type: &ConcreteDataType,
    opts: &[ColumnOptionDef],
) -> Result<Option<ColumnDefaultConstraint>> {
    if let Some(opt) = opts
        .iter()
        .find(|o| matches!(o.option, ColumnOption::Default(_)))
    {
        let default_constraint = match &opt.option {
            ColumnOption::Default(Expr::Value(v)) => {
                ColumnDefaultConstraint::Value(sql_value_to_value(column_name, data_type, v)?)
            }
            ColumnOption::Default(Expr::Function(func)) => {
                // Always use lowercase for function expression
                ColumnDefaultConstraint::Function(format!("{}", func).to_lowercase())
            }
            ColumnOption::Default(expr) => {
                return UnsupportedDefaultValueSnafu {
                    column_name,
                    expr: expr.clone(),
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

// TODO(yingwen): Make column nullable by default, and checks invalid case like
// a column is not nullable but has a default value null.
/// Create a `ColumnSchema` from `ColumnDef`.
pub fn column_def_to_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let is_nullable = column_def
        .options
        .iter()
        .any(|o| matches!(o.option, ColumnOption::Null));

    let name = column_def.name.value.clone();
    let data_type = sql_data_type_to_concrete_data_type(&column_def.data_type)?;
    let default_constraint =
        parse_column_default_constraint(&name, &data_type, &column_def.options)?;

    ColumnSchema::new(name, data_type, is_nullable)
        .with_default_constraint(default_constraint)
        .context(error::InvalidDefaultSnafu {
            column: &column_def.name.value,
        })
}

pub fn sql_data_type_to_concrete_data_type(data_type: &SqlDataType) -> Result<ConcreteDataType> {
    match data_type {
        SqlDataType::BigInt(_) => Ok(ConcreteDataType::int64_datatype()),
        SqlDataType::Int(_) => Ok(ConcreteDataType::int32_datatype()),
        SqlDataType::SmallInt(_) => Ok(ConcreteDataType::int16_datatype()),
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::String => Ok(ConcreteDataType::string_datatype()),
        SqlDataType::Float(_) => Ok(ConcreteDataType::float32_datatype()),
        SqlDataType::Double => Ok(ConcreteDataType::float64_datatype()),
        SqlDataType::Boolean => Ok(ConcreteDataType::boolean_datatype()),
        SqlDataType::Date => Ok(ConcreteDataType::date_datatype()),
        SqlDataType::Custom(obj_name) => match &obj_name.0[..] {
            [type_name] => {
                if type_name.value.eq_ignore_ascii_case(DateTimeType::name()) {
                    Ok(ConcreteDataType::datetime_datatype())
                } else {
                    error::SqlTypeNotSupportedSnafu {
                        t: data_type.clone(),
                    }
                    .fail()
                }
            }
            _ => error::SqlTypeNotSupportedSnafu {
                t: data_type.clone(),
            }
            .fail(),
        },
        SqlDataType::Timestamp => Ok(ConcreteDataType::timestamp_millis_datatype()),
        _ => error::SqlTypeNotSupportedSnafu {
            t: data_type.clone(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use common_time::timestamp::TimeUnit;
    use datatypes::value::OrderedFloat;

    use super::*;
    use crate::ast::Ident;

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
            SqlDataType::SmallInt(None),
            ConcreteDataType::int16_datatype(),
        );
        check_type(SqlDataType::Char(None), ConcreteDataType::string_datatype());
        check_type(
            SqlDataType::Varchar(None),
            ConcreteDataType::string_datatype(),
        );
        check_type(SqlDataType::Text, ConcreteDataType::string_datatype());
        check_type(SqlDataType::String, ConcreteDataType::string_datatype());
        check_type(
            SqlDataType::Float(None),
            ConcreteDataType::float32_datatype(),
        );
        check_type(SqlDataType::Double, ConcreteDataType::float64_datatype());
        check_type(SqlDataType::Boolean, ConcreteDataType::boolean_datatype());
        check_type(SqlDataType::Date, ConcreteDataType::date_datatype());
        check_type(
            SqlDataType::Custom(ObjectName(vec![Ident::new("datetime")])),
            ConcreteDataType::datetime_datatype(),
        );
        check_type(
            SqlDataType::Timestamp,
            ConcreteDataType::timestamp_millis_datatype(),
        );
    }

    #[test]
    fn test_sql_number_to_value() {
        let v = sql_number_to_value(&ConcreteDataType::float64_datatype(), "3.0").unwrap();
        assert_eq!(Value::Float64(OrderedFloat(3.0)), v);

        let v = sql_number_to_value(&ConcreteDataType::int32_datatype(), "999").unwrap();
        assert_eq!(Value::Int32(999), v);

        let v = sql_number_to_value(&ConcreteDataType::string_datatype(), "999");
        assert!(v.is_err(), "parse value error is: {:?}", v);
    }

    #[test]
    fn test_sql_value_to_value() {
        let sql_val = SqlValue::Null;
        assert_eq!(
            Value::Null,
            sql_value_to_value("a", &ConcreteDataType::float64_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Boolean(true);
        assert_eq!(
            Value::Boolean(true),
            sql_value_to_value("a", &ConcreteDataType::boolean_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        assert_eq!(
            Value::Float64(OrderedFloat(3.0)),
            sql_value_to_value("a", &ConcreteDataType::float64_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        let v = sql_value_to_value("a", &ConcreteDataType::boolean_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{:?}", v)
            .contains("Fail to parse number 3.0, invalid column type: Boolean(BooleanType)"));

        let sql_val = SqlValue::Boolean(true);
        let v = sql_value_to_value("a", &ConcreteDataType::float64_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{:?}", v).contains(
            "column_name: \"a\", expect: Float64(Float64), actual: Boolean(BooleanType)"
        ));
    }

    #[test]
    pub fn test_parse_date_literal() {
        let value = sql_value_to_value(
            "date",
            &ConcreteDataType::date_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22".to_string()),
        )
        .unwrap();
        assert_eq!(ConcreteDataType::date_datatype(), value.data_type());
        if let Value::Date(d) = value {
            assert_eq!("2022-02-22", d.to_string());
        } else {
            unreachable!()
        }
    }

    #[test]
    pub fn test_parse_datetime_literal() {
        let value = sql_value_to_value(
            "datetime_col",
            &ConcreteDataType::datetime_datatype(),
            &SqlValue::DoubleQuotedString("2022-02-22 00:01:03".to_string()),
        )
        .unwrap();
        assert_eq!(ConcreteDataType::datetime_datatype(), value.data_type());
        if let Value::DateTime(d) = value {
            assert_eq!("2022-02-22 00:01:03", d.to_string());
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
        )
        .is_err());
    }

    #[test]
    fn test_parse_timestamp_literal() {
        match parse_string_to_value(
            "timestamp_col",
            "2022-02-22T00:01:01+08:00".to_string(),
            &ConcreteDataType::timestamp_millis_datatype(),
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
        )
        .is_err());
    }
}
