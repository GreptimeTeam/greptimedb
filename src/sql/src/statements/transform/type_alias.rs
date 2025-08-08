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

use datatypes::data_type::DataType as GreptimeDataType;
use sqlparser::ast::{
    DataType, ExactNumberInfo, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList,
    Ident, ObjectName, Value,
};

use crate::ast::ObjectNamePartExt;
use crate::error::Result;
use crate::statements::alter::AlterTableOperation;
use crate::statements::create::{CreateExternalTable, CreateTable};
use crate::statements::statement::Statement;
use crate::statements::transform::TransformRule;
use crate::statements::{sql_data_type_to_concrete_data_type, TimezoneInfo};

/// SQL data type alias transformer:
///  - `TimestampSecond`, `Timestamp_s`, `Timestamp_sec` for `Timestamp(0)`.
///  - `TimestampMillisecond`, `Timestamp_ms` for `Timestamp(3)`.
///  - `TimestampMicrosecond`, `Timestamp_us` for `Timestamp(6)`.
///  - `TimestampNanosecond`, `Timestamp_ns` for `Timestamp(9)`.
///  - `INT8` for `tinyint`
///  - `INT16` for `smallint`
///  - `INT32` for `int`
///  - `INT64` for `bigint`
///  -  And `UINT8`, `UINT16` etc. for `TinyIntUnsigned` etc.
///  -  TinyText, MediumText, LongText for `Text`.
pub(crate) struct TypeAliasTransformRule;

impl TransformRule for TypeAliasTransformRule {
    fn visit_statement(&self, stmt: &mut Statement) -> Result<()> {
        match stmt {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                columns
                    .iter_mut()
                    .for_each(|column| replace_type_alias(column.mut_data_type()));
            }
            Statement::CreateExternalTable(CreateExternalTable { columns, .. }) => {
                columns
                    .iter_mut()
                    .for_each(|column| replace_type_alias(column.mut_data_type()));
            }
            Statement::AlterTable(alter_table) => {
                if let AlterTableOperation::ModifyColumnType { target_type, .. } =
                    alter_table.alter_operation_mut()
                {
                    replace_type_alias(target_type)
                } else if let AlterTableOperation::AddColumns { add_columns, .. } =
                    alter_table.alter_operation_mut()
                {
                    for add_column in add_columns {
                        replace_type_alias(&mut add_column.column_def.data_type);
                    }
                }
            }
            _ => {}
        }

        Ok(())
    }

    fn visit_expr(&self, expr: &mut Expr) -> ControlFlow<()> {
        fn cast_expr_to_arrow_cast_func(expr: Expr, cast_type: String) -> Function {
            Function {
                name: ObjectName::from(vec![Ident::new("arrow_cast")]),
                args: sqlparser::ast::FunctionArguments::List(FunctionArgumentList {
                    args: vec![
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)),
                        FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                            Value::SingleQuotedString(cast_type).into(),
                        ))),
                    ],
                    duplicate_treatment: None,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                parameters: sqlparser::ast::FunctionArguments::None,
                within_group: vec![],
                uses_odbc_syntax: false,
            }
        }

        match expr {
            // In new sqlparser, the "INT64" is no longer parsed to custom datatype.
            // The new "Int64" is not recognizable by Datafusion, cannot directly "CAST" to it.
            // We have to replace the expr to "arrow_cast" function call here.
            // Same for "FLOAT64".
            Expr::Cast {
                expr: cast_expr,
                data_type,
                ..
            } if get_type_by_alias(data_type).is_some() => {
                // Safety: checked in the match arm.
                let new_type = get_type_by_alias(data_type).unwrap();
                if let Ok(new_type) = sql_data_type_to_concrete_data_type(&new_type) {
                    *expr = Expr::Function(cast_expr_to_arrow_cast_func(
                        (**cast_expr).clone(),
                        new_type.as_arrow_type().to_string(),
                    ));
                }
            }

            // Timestamp(precision) in cast, datafusion doesn't support Timestamp(9) etc.
            // We have to transform it into arrow_cast(expr, type).
            Expr::Cast {
                data_type: DataType::Timestamp(precision, zone),
                expr: cast_expr,
                ..
            } => {
                if let Ok(concrete_type) =
                    sql_data_type_to_concrete_data_type(&DataType::Timestamp(*precision, *zone))
                {
                    let new_type = concrete_type.as_arrow_type();
                    *expr = Expr::Function(cast_expr_to_arrow_cast_func(
                        (**cast_expr).clone(),
                        new_type.to_string(),
                    ));
                }
            }

            // TODO(dennis): supports try_cast
            _ => {}
        }

        ControlFlow::<()>::Continue(())
    }
}

fn replace_type_alias(data_type: &mut DataType) {
    if let Some(new_type) = get_type_by_alias(data_type) {
        *data_type = new_type;
    }
}

/// Get data type from alias type.
/// Returns the mapped data type if the input data type is an alias that we need to replace.
// Remember to update `get_data_type_by_alias_name()` if you modify this method.
pub(crate) fn get_type_by_alias(data_type: &DataType) -> Option<DataType> {
    match data_type {
        // The sqlparser latest version contains the Int8 alias for Postgres Bigint.
        // Which means 8 bytes in postgres (not 8 bits).
        // See https://docs.rs/sqlparser/latest/sqlparser/ast/enum.DataType.html#variant.Int8
        DataType::Custom(name, tokens) if name.0.len() == 1 && tokens.is_empty() => {
            get_data_type_by_alias_name(name.0[0].to_string_unquoted().as_str())
        }
        DataType::Int8(None) => Some(DataType::TinyInt(None)),
        DataType::Int16 => Some(DataType::SmallInt(None)),
        DataType::Int32 => Some(DataType::Int(None)),
        DataType::Int64 => Some(DataType::BigInt(None)),
        DataType::UInt8 => Some(DataType::TinyIntUnsigned(None)),
        DataType::UInt16 => Some(DataType::SmallIntUnsigned(None)),
        DataType::UInt32 => Some(DataType::IntUnsigned(None)),
        DataType::UInt64 => Some(DataType::BigIntUnsigned(None)),
        DataType::Float32 => Some(DataType::Float(None)),
        DataType::Float64 => Some(DataType::Double(ExactNumberInfo::None)),
        DataType::Bool => Some(DataType::Boolean),
        DataType::Datetime(_) => Some(DataType::Timestamp(Some(6), TimezoneInfo::None)),
        _ => None,
    }
}

/// Get the mapped data type from alias name.
/// It only supports the following types of alias:
/// - timestamps
/// - ints
/// - floats
/// - texts
// Remember to update `get_type_alias()` if you modify this method.
pub(crate) fn get_data_type_by_alias_name(name: &str) -> Option<DataType> {
    match name.to_uppercase().as_ref() {
        // Timestamp type alias
        "TIMESTAMP_S" | "TIMESTAMP_SEC" | "TIMESTAMPSECOND" => {
            Some(DataType::Timestamp(Some(0), TimezoneInfo::None))
        }

        "TIMESTAMP_MS" | "TIMESTAMPMILLISECOND" => {
            Some(DataType::Timestamp(Some(3), TimezoneInfo::None))
        }
        "TIMESTAMP_US" | "TIMESTAMPMICROSECOND" | "DATETIME" => {
            Some(DataType::Timestamp(Some(6), TimezoneInfo::None))
        }
        "TIMESTAMP_NS" | "TIMESTAMPNANOSECOND" => {
            Some(DataType::Timestamp(Some(9), TimezoneInfo::None))
        }
        // Number type alias
        // We keep them for backward compatibility.
        "INT8" => Some(DataType::TinyInt(None)),
        "INT16" => Some(DataType::SmallInt(None)),
        "INT32" => Some(DataType::Int(None)),
        "INT64" => Some(DataType::BigInt(None)),
        "UINT8" => Some(DataType::TinyIntUnsigned(None)),
        "UINT16" => Some(DataType::SmallIntUnsigned(None)),
        "UINT32" => Some(DataType::IntUnsigned(None)),
        "UINT64" => Some(DataType::BigIntUnsigned(None)),
        "FLOAT32" => Some(DataType::Float(None)),
        "FLOAT64" => Some(DataType::Double(ExactNumberInfo::None)),
        // String type alias
        "TINYTEXT" | "MEDIUMTEXT" | "LONGTEXT" => Some(DataType::Text),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::{ParseOptions, ParserContext};
    use crate::statements::transform_statements;

    #[test]
    fn test_get_data_type_by_alias_name() {
        assert_eq!(
            get_data_type_by_alias_name("float64"),
            Some(DataType::Double(ExactNumberInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Float64"),
            Some(DataType::Double(ExactNumberInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("FLOAT64"),
            Some(DataType::Double(ExactNumberInfo::None))
        );

        assert_eq!(
            get_data_type_by_alias_name("float32"),
            Some(DataType::Float(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("int8"),
            Some(DataType::TinyInt(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("INT16"),
            Some(DataType::SmallInt(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("INT32"),
            Some(DataType::Int(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("INT64"),
            Some(DataType::BigInt(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Uint8"),
            Some(DataType::TinyIntUnsigned(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("UINT16"),
            Some(DataType::SmallIntUnsigned(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("UINT32"),
            Some(DataType::IntUnsigned(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("uint64"),
            Some(DataType::BigIntUnsigned(None))
        );

        assert_eq!(
            get_data_type_by_alias_name("TimestampSecond"),
            Some(DataType::Timestamp(Some(0), TimezoneInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Timestamp_s"),
            Some(DataType::Timestamp(Some(0), TimezoneInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Timestamp_sec"),
            Some(DataType::Timestamp(Some(0), TimezoneInfo::None))
        );

        assert_eq!(
            get_data_type_by_alias_name("TimestampMilliSecond"),
            Some(DataType::Timestamp(Some(3), TimezoneInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Timestamp_ms"),
            Some(DataType::Timestamp(Some(3), TimezoneInfo::None))
        );

        assert_eq!(
            get_data_type_by_alias_name("TimestampMicroSecond"),
            Some(DataType::Timestamp(Some(6), TimezoneInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Timestamp_us"),
            Some(DataType::Timestamp(Some(6), TimezoneInfo::None))
        );

        assert_eq!(
            get_data_type_by_alias_name("TimestampNanoSecond"),
            Some(DataType::Timestamp(Some(9), TimezoneInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("Timestamp_ns"),
            Some(DataType::Timestamp(Some(9), TimezoneInfo::None))
        );
        assert_eq!(
            get_data_type_by_alias_name("TinyText"),
            Some(DataType::Text)
        );
        assert_eq!(
            get_data_type_by_alias_name("MediumText"),
            Some(DataType::Text)
        );
        assert_eq!(
            get_data_type_by_alias_name("LongText"),
            Some(DataType::Text)
        );
    }

    fn test_timestamp_alias(alias: &str, expected: &str) {
        let sql = format!("SELECT TIMESTAMP '2020-01-01 01:23:45.12345678'::{alias}");
        let mut stmts =
            ParserContext::create_with_dialect(&sql, &GenericDialect {}, ParseOptions::default())
                .unwrap();
        transform_statements(&mut stmts).unwrap();

        match &stmts[0] {
            Statement::Query(q) => assert_eq!(format!("SELECT arrow_cast(TIMESTAMP '2020-01-01 01:23:45.12345678', 'Timestamp({expected}, None)')"), q.to_string()),
            _ => unreachable!(),
        }
    }

    fn test_timestamp_precision_type(precision: i32, expected: &str) {
        test_timestamp_alias(&format!("Timestamp({precision})"), expected);
    }

    #[test]
    fn test_boolean_alias() {
        let sql = "CREATE TABLE test(b bool, ts TIMESTAMP TIME INDEX)";
        let mut stmts =
            ParserContext::create_with_dialect(sql, &GenericDialect {}, ParseOptions::default())
                .unwrap();
        transform_statements(&mut stmts).unwrap();

        match &stmts[0] {
            Statement::CreateTable(c) => assert_eq!("CREATE TABLE test (\n  b BOOLEAN,\n  ts TIMESTAMP NOT NULL,\n  TIME INDEX (ts)\n)\nENGINE=mito\n", c.to_string()),
            _ => unreachable!(),
        }
    }

    #[test]
    fn test_transform_timestamp_alias() {
        // Timestamp[Second | Millisecond | Microsecond | Nanosecond]
        test_timestamp_alias("TimestampSecond", "Second");
        test_timestamp_alias("Timestamp_s", "Second");
        test_timestamp_alias("TimestampMillisecond", "Millisecond");
        test_timestamp_alias("Timestamp_ms", "Millisecond");
        test_timestamp_alias("TimestampMicrosecond", "Microsecond");
        test_timestamp_alias("Timestamp_us", "Microsecond");
        test_timestamp_alias("TimestampNanosecond", "Nanosecond");
        test_timestamp_alias("Timestamp_ns", "Nanosecond");
        // Timestamp(precision)
        test_timestamp_precision_type(0, "Second");
        test_timestamp_precision_type(3, "Millisecond");
        test_timestamp_precision_type(6, "Microsecond");
        test_timestamp_precision_type(9, "Nanosecond");
    }

    #[test]
    fn test_create_sql_with_type_alias() {
        let sql = r#"
CREATE TABLE data_types (
  s string,
  tt tinytext,
  mt mediumtext,
  lt longtext,
  tint int8,
  sint int16,
  i int32,
  bint int64,
  v varchar,
  f float32,
  d float64,
  b boolean,
  vb varbinary,
  dt date,
  dtt datetime,
  ts0 TimestampSecond,
  ts3 TimestampMillisecond,
  ts6 TimestampMicrosecond,
  ts9 TimestampNanosecond DEFAULT CURRENT_TIMESTAMP TIME INDEX,
  PRIMARY KEY(s));"#;

        let mut stmts =
            ParserContext::create_with_dialect(sql, &GenericDialect {}, ParseOptions::default())
                .unwrap();
        transform_statements(&mut stmts).unwrap();

        match &stmts[0] {
            Statement::CreateTable(c) => {
                let expected = r#"CREATE TABLE data_types (
  s STRING,
  tt TINYTEXT,
  mt MEDIUMTEXT,
  lt LONGTEXT,
  tint TINYINT,
  sint SMALLINT,
  i INT,
  bint BIGINT,
  v VARCHAR,
  f FLOAT,
  d DOUBLE,
  b BOOLEAN,
  vb VARBINARY,
  dt DATE,
  dtt TIMESTAMP(6),
  ts0 TIMESTAMP(0),
  ts3 TIMESTAMP(3),
  ts6 TIMESTAMP(6),
  ts9 TIMESTAMP(9) DEFAULT CURRENT_TIMESTAMP NOT NULL,
  TIME INDEX (ts9),
  PRIMARY KEY (s)
)
ENGINE=mito
"#;

                assert_eq!(expected, c.to_string());
            }
            _ => unreachable!(),
        }
    }
}
