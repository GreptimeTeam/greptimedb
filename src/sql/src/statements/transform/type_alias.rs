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
    ColumnDef, DataType, Expr, Function, FunctionArg, FunctionArgExpr, Ident, ObjectName, Value,
};

use crate::error::Result;
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
///  -  And `UINT8`, `UINT16` etc. for `UnsignedTinyint` etc.
pub(crate) struct TypeAliasTransformRule;

impl TransformRule for TypeAliasTransformRule {
    fn visit_statement(&self, stmt: &mut Statement) -> Result<()> {
        match stmt {
            Statement::CreateTable(CreateTable { columns, .. }) => {
                columns
                    .iter_mut()
                    .for_each(|ColumnDef { data_type, .. }| replace_type_alias(data_type));
            }
            Statement::CreateExternalTable(CreateExternalTable { columns, .. }) => {
                columns
                    .iter_mut()
                    .for_each(|ColumnDef { data_type, .. }| replace_type_alias(data_type));
            }
            _ => {}
        }

        Ok(())
    }

    fn visit_expr(&self, expr: &mut Expr) -> ControlFlow<()> {
        match expr {
            // Type alias
            Expr::Cast {
                data_type: DataType::Custom(name, tokens),
                expr: cast_expr,
            } if name.0.len() == 1 && tokens.is_empty() => {
                if let Some(new_type) = get_data_type_by_alias_name(name.0[0].value.as_str()) {
                    if let Ok(concrete_type) = sql_data_type_to_concrete_data_type(&new_type) {
                        let new_type = concrete_type.as_arrow_type();
                        *expr = Expr::Function(Function {
                            name: ObjectName(vec![Ident::new("arrow_cast")]),
                            args: vec![
                                FunctionArg::Unnamed(FunctionArgExpr::Expr((**cast_expr).clone())),
                                FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                    Value::SingleQuotedString(new_type.to_string()),
                                ))),
                            ],
                            over: None,
                            distinct: false,
                            special: false,
                            order_by: vec![],
                        });
                    }
                }
            }

            // Timestamp(precision) in cast, datafusion doesn't support Timestamp(9) etc.
            // We have to transform it into arrow_cast(expr, type).
            Expr::Cast {
                data_type: DataType::Timestamp(precision, zone),
                expr: cast_expr,
            } => {
                if let Ok(concrete_type) =
                    sql_data_type_to_concrete_data_type(&DataType::Timestamp(*precision, *zone))
                {
                    let new_type = concrete_type.as_arrow_type();
                    *expr = Expr::Function(Function {
                        name: ObjectName(vec![Ident::new("arrow_cast")]),
                        args: vec![
                            FunctionArg::Unnamed(FunctionArgExpr::Expr((**cast_expr).clone())),
                            FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Value(
                                Value::SingleQuotedString(new_type.to_string()),
                            ))),
                        ],
                        over: None,
                        distinct: false,
                        special: false,
                        order_by: vec![],
                    });
                }
            }

            // TODO(dennis): supports try_cast
            _ => {}
        }

        ControlFlow::<()>::Continue(())
    }
}

fn replace_type_alias(data_type: &mut DataType) {
    match data_type {
        // TODO(dennis): The sqlparser latest version contains the Int8 alias for postres Bigint.
        // Which means 8 bytes in postgres (not 8 bits). If we upgrade the sqlparser, need to process it.
        // See https://docs.rs/sqlparser/latest/sqlparser/ast/enum.DataType.html#variant.Int8
        DataType::Custom(name, tokens) if name.0.len() == 1 && tokens.is_empty() => {
            if let Some(new_type) = get_data_type_by_alias_name(name.0[0].value.as_str()) {
                *data_type = new_type;
            }
        }
        _ => {}
    }
}

pub fn get_data_type_by_alias_name(name: &str) -> Option<DataType> {
    match name.to_uppercase().as_ref() {
        // Timestamp type alias
        "TIMESTAMP_S" | "TIMESTAMP_SEC" | "TIMESTAMPSECOND" => {
            Some(DataType::Timestamp(Some(0), TimezoneInfo::None))
        }

        "TIMESTAMP_MS" | "TIMESTAMPMILLISECOND" => {
            Some(DataType::Timestamp(Some(3), TimezoneInfo::None))
        }
        "TIMESTAMP_US" | "TIMESTAMPMICROSECOND" => {
            Some(DataType::Timestamp(Some(6), TimezoneInfo::None))
        }
        "TIMESTAMP_NS" | "TIMESTAMPNANOSECOND" => {
            Some(DataType::Timestamp(Some(9), TimezoneInfo::None))
        }
        // Number type alias
        "INT8" => Some(DataType::TinyInt(None)),
        "INT16" => Some(DataType::SmallInt(None)),
        "INT32" => Some(DataType::Int(None)),
        "INT64" => Some(DataType::BigInt(None)),
        "UINT8" => Some(DataType::UnsignedTinyInt(None)),
        "UINT16" => Some(DataType::UnsignedSmallInt(None)),
        "UINT32" => Some(DataType::UnsignedInt(None)),
        "UINT64" => Some(DataType::UnsignedBigInt(None)),
        "FLOAT32" => Some(DataType::Float(None)),
        "FLOAT64" => Some(DataType::Double),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;

    use super::*;
    use crate::parser::ParserContext;
    use crate::statements::transform_statements;

    #[test]
    fn test_get_data_type_by_alias_name() {
        assert_eq!(
            get_data_type_by_alias_name("float64"),
            Some(DataType::Double)
        );
        assert_eq!(
            get_data_type_by_alias_name("Float64"),
            Some(DataType::Double)
        );
        assert_eq!(
            get_data_type_by_alias_name("FLOAT64"),
            Some(DataType::Double)
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
            Some(DataType::UnsignedTinyInt(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("UINT16"),
            Some(DataType::UnsignedSmallInt(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("UINT32"),
            Some(DataType::UnsignedInt(None))
        );
        assert_eq!(
            get_data_type_by_alias_name("uint64"),
            Some(DataType::UnsignedBigInt(None))
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
    }

    fn test_timestamp_alias(alias: &str, expected: &str) {
        let sql = format!("SELECT TIMESTAMP '2020-01-01 01:23:45.12345678'::{alias}");
        let mut stmts = ParserContext::create_with_dialect(&sql, &GenericDialect {}).unwrap();
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

        let mut stmts = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        transform_statements(&mut stmts).unwrap();

        match &stmts[0] {
            Statement::CreateTable(c) => {
                let expected = r#"CREATE TABLE  data_types (
  s STRING,
  tint INT8,
  sint SMALLINT,
  i INT,
  bint BIGINT,
  v VARCHAR,
  f FLOAT,
  d DOUBLE,
  b BOOLEAN,
  vb VARBINARY,
  dt DATE,
  dtt DATETIME,
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
