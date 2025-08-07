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

pub mod admin;
pub mod alter;
pub mod copy;
pub mod create;
pub mod cursor;
pub mod delete;
pub mod describe;
pub mod drop;
pub mod explain;
pub mod insert;
pub mod kill;
mod option_map;
pub mod query;
pub mod set_variables;
pub mod show;
pub mod statement;
pub mod tql;
pub(crate) mod transform;
pub mod truncate;

use api::helper::ColumnDataTypeWrapper;
use api::v1::SemanticType;
use common_sql::default_constraint::parse_column_default_constraint;
use common_time::timezone::Timezone;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, COMMENT_KEY};
use datatypes::types::TimestampType;
use datatypes::value::Value;
use snafu::ResultExt;
use sqlparser::ast::{ExactNumberInfo, Ident};

use crate::ast::{
    ColumnDef, ColumnOption, DataType as SqlDataType, ObjectNamePartExt, TimezoneInfo,
    Value as SqlValue,
};
use crate::error::{
    self, ConvertToGrpcDataTypeSnafu, ConvertValueSnafu, Result,
    SerializeColumnDefaultConstraintSnafu, SetFulltextOptionSnafu, SetSkippingIndexOptionSnafu,
    SqlCommonSnafu,
};
use crate::statements::create::Column;
pub use crate::statements::option_map::OptionMap;
pub(crate) use crate::statements::transform::transform_statements;

const VECTOR_TYPE_NAME: &str = "VECTOR";

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
        Value::Timestamp(ts) => SqlValue::SingleQuotedString(ts.to_iso8601_string()),
        Value::String(s) => SqlValue::SingleQuotedString(s.as_utf8().to_string()),
        Value::Null => SqlValue::Null,
        // TODO(dennis): supports binary
        _ => return ConvertValueSnafu { value: val.clone() }.fail(),
    })
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

/// Create a `ColumnSchema` from `Column`.
pub fn column_to_schema(
    column: &Column,
    time_index: &str,
    timezone: Option<&Timezone>,
) -> Result<ColumnSchema> {
    let is_time_index = column.name().value == time_index;

    let is_nullable = column
        .options()
        .iter()
        .all(|o| !matches!(o.option, ColumnOption::NotNull))
        && !is_time_index;

    let name = column.name().value.clone();
    let data_type = sql_data_type_to_concrete_data_type(column.data_type())?;
    let default_constraint =
        parse_column_default_constraint(&name, &data_type, column.options(), timezone)
            .context(SqlCommonSnafu)?;

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

    if let Some(options) = column.extensions.build_skipping_index_options()? {
        column_schema = column_schema
            .with_skipping_options(options)
            .context(SetSkippingIndexOptionSnafu)?;
    }

    column_schema.set_inverted_index(column.extensions.inverted_index_options.is_some());

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
        parse_column_default_constraint(&name, &data_type, &col.options, timezone)
            .context(SqlCommonSnafu)?
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
        SqlDataType::BigIntUnsigned(_) => Ok(ConcreteDataType::uint64_datatype()),
        SqlDataType::Int(_) | SqlDataType::Integer(_) => Ok(ConcreteDataType::int32_datatype()),
        SqlDataType::IntUnsigned(_) | SqlDataType::UnsignedInteger => {
            Ok(ConcreteDataType::uint32_datatype())
        }
        SqlDataType::SmallInt(_) => Ok(ConcreteDataType::int16_datatype()),
        SqlDataType::SmallIntUnsigned(_) => Ok(ConcreteDataType::uint16_datatype()),
        SqlDataType::TinyInt(_) | SqlDataType::Int8(_) => Ok(ConcreteDataType::int8_datatype()),
        SqlDataType::TinyIntUnsigned(_) | SqlDataType::Int8Unsigned(_) => {
            Ok(ConcreteDataType::uint8_datatype())
        }
        SqlDataType::Char(_)
        | SqlDataType::Varchar(_)
        | SqlDataType::Text
        | SqlDataType::TinyText
        | SqlDataType::MediumText
        | SqlDataType::LongText
        | SqlDataType::String(_) => Ok(ConcreteDataType::string_datatype()),
        SqlDataType::Float(_) => Ok(ConcreteDataType::float32_datatype()),
        SqlDataType::Double(_) | SqlDataType::Float64 => Ok(ConcreteDataType::float64_datatype()),
        SqlDataType::Boolean => Ok(ConcreteDataType::boolean_datatype()),
        SqlDataType::Date => Ok(ConcreteDataType::date_datatype()),
        SqlDataType::Binary(_)
        | SqlDataType::Blob(_)
        | SqlDataType::Bytea
        | SqlDataType::Varbinary(_) => Ok(ConcreteDataType::binary_datatype()),
        SqlDataType::Datetime(_) => Ok(ConcreteDataType::timestamp_microsecond_datatype()),
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
        SqlDataType::JSON => Ok(ConcreteDataType::json_datatype()),
        // Vector type
        SqlDataType::Custom(name, d)
            if name.0.as_slice().len() == 1
                && name.0.as_slice()[0]
                    .to_string_unquoted()
                    .to_ascii_uppercase()
                    == VECTOR_TYPE_NAME
                && d.len() == 1 =>
        {
            let dim = d[0].parse().map_err(|e| {
                error::ParseSqlValueSnafu {
                    msg: format!("Failed to parse vector dimension: {}", e),
                }
                .build()
            })?;
            Ok(ConcreteDataType::vector_datatype(dim))
        }
        _ => error::SqlTypeNotSupportedSnafu {
            t: data_type.clone(),
        }
        .fail(),
    }
}

pub fn concrete_data_type_to_sql_data_type(data_type: &ConcreteDataType) -> Result<SqlDataType> {
    match data_type {
        ConcreteDataType::Int64(_) => Ok(SqlDataType::BigInt(None)),
        ConcreteDataType::UInt64(_) => Ok(SqlDataType::BigIntUnsigned(None)),
        ConcreteDataType::Int32(_) => Ok(SqlDataType::Int(None)),
        ConcreteDataType::UInt32(_) => Ok(SqlDataType::IntUnsigned(None)),
        ConcreteDataType::Int16(_) => Ok(SqlDataType::SmallInt(None)),
        ConcreteDataType::UInt16(_) => Ok(SqlDataType::SmallIntUnsigned(None)),
        ConcreteDataType::Int8(_) => Ok(SqlDataType::TinyInt(None)),
        ConcreteDataType::UInt8(_) => Ok(SqlDataType::TinyIntUnsigned(None)),
        ConcreteDataType::String(_) => Ok(SqlDataType::String(None)),
        ConcreteDataType::Float32(_) => Ok(SqlDataType::Float(None)),
        ConcreteDataType::Float64(_) => Ok(SqlDataType::Double(ExactNumberInfo::None)),
        ConcreteDataType::Boolean(_) => Ok(SqlDataType::Boolean),
        ConcreteDataType::Date(_) => Ok(SqlDataType::Date),
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
        ConcreteDataType::Json(_) => Ok(SqlDataType::JSON),
        ConcreteDataType::Vector(v) => Ok(SqlDataType::Custom(
            vec![Ident::new(VECTOR_TYPE_NAME)].into(),
            vec![v.dim.to_string()],
        )),
        ConcreteDataType::Duration(_)
        | ConcreteDataType::Null(_)
        | ConcreteDataType::List(_)
        | ConcreteDataType::Struct(_)
        | ConcreteDataType::Dictionary(_) => error::ConcreteTypeNotSupportedSnafu {
            t: data_type.clone(),
        }
        .fail(),
    }
}

#[cfg(test)]
mod tests {
    use api::v1::ColumnDataType;
    use datatypes::schema::{
        FulltextAnalyzer, COLUMN_FULLTEXT_OPT_KEY_ANALYZER, COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE,
    };
    use sqlparser::ast::{ColumnOptionDef, Expr};

    use super::*;
    use crate::ast::TimezoneInfo;
    use crate::statements::create::ColumnExtensions;
    use crate::statements::ColumnOption;

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
        check_type(
            SqlDataType::Double(ExactNumberInfo::None),
            ConcreteDataType::float64_datatype(),
        );
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
            SqlDataType::BigIntUnsigned(None),
            ConcreteDataType::uint64_datatype(),
        );
        check_type(
            SqlDataType::IntUnsigned(None),
            ConcreteDataType::uint32_datatype(),
        );
        check_type(
            SqlDataType::SmallIntUnsigned(None),
            ConcreteDataType::uint16_datatype(),
        );
        check_type(
            SqlDataType::TinyIntUnsigned(None),
            ConcreteDataType::uint8_datatype(),
        );
        check_type(
            SqlDataType::Datetime(None),
            ConcreteDataType::timestamp_microsecond_datatype(),
        );
        check_type(
            SqlDataType::Interval,
            ConcreteDataType::interval_month_day_nano_datatype(),
        );
        check_type(SqlDataType::JSON, ConcreteDataType::json_datatype());
        check_type(
            SqlDataType::Custom(
                vec![Ident::new(VECTOR_TYPE_NAME)].into(),
                vec!["3".to_string()],
            ),
            ConcreteDataType::vector_datatype(3),
        );
    }

    #[test]
    pub fn test_sql_column_def_to_grpc_column_def() {
        // test basic
        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double(ExactNumberInfo::None),
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
            data_type: SqlDataType::Double(ExactNumberInfo::None),
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
            data_type: SqlDataType::Double(ExactNumberInfo::None),
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
            options: vec![ColumnOptionDef {
                name: None,
                option: ColumnOption::Default(Expr::Value(
                    SqlValue::SingleQuotedString("2024-01-30T00:01:01".to_string()).into(),
                )),
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
            data_type: SqlDataType::Double(ExactNumberInfo::None),
            options: vec![],
        };
        assert!(!has_primary_key_option(&column_def));

        let column_def = ColumnDef {
            name: "col".into(),
            data_type: SqlDataType::Double(ExactNumberInfo::None),
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
                data_type: SqlDataType::Double(ExactNumberInfo::None),
                options: vec![],
            },
            extensions: ColumnExtensions::default(),
        };

        let column_schema = column_to_schema(&column_def, "ts", None).unwrap();

        assert_eq!("col", column_schema.name);
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            column_schema.data_type
        );
        assert!(column_schema.is_nullable());
        assert!(!column_schema.is_time_index());

        let column_schema = column_to_schema(&column_def, "col", None).unwrap();

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

        let column_schema = column_to_schema(&column_def, "ts", None).unwrap();

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
                options: vec![ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Default(Expr::Value(
                        SqlValue::SingleQuotedString("2024-01-30T00:01:01".to_string()).into(),
                    )),
                }],
            },
            extensions: ColumnExtensions::default(),
        };

        // with timezone "Asia/Shanghai"

        let column_schema = column_to_schema(
            &column,
            "ts",
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
        let column_schema = column_to_schema(&column, "ts", None).unwrap();

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
                options: vec![],
            },
            extensions: ColumnExtensions {
                fulltext_index_options: Some(OptionMap::from([
                    (
                        COLUMN_FULLTEXT_OPT_KEY_ANALYZER.to_string(),
                        "English".to_string(),
                    ),
                    (
                        COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE.to_string(),
                        "true".to_string(),
                    ),
                ])),
                vector_options: None,
                skipping_index_options: None,
                inverted_index_options: None,
            },
        };

        let column_schema = column_to_schema(&column, "ts", None).unwrap();
        assert_eq!("col", column_schema.name);
        assert_eq!(ConcreteDataType::string_datatype(), column_schema.data_type);
        let fulltext_options = column_schema.fulltext_options().unwrap().unwrap();
        assert_eq!(fulltext_options.analyzer, FulltextAnalyzer::English);
        assert!(fulltext_options.case_sensitive);
    }
}
