pub mod alter;
pub mod create_table;
pub mod insert;
pub mod query;
pub mod show_database;
pub mod show_kind;
pub mod statement;

use datatypes::prelude::ConcreteDataType;
use datatypes::schema::ColumnSchema;
use datatypes::types::DateTimeType;

use crate::ast::{ColumnDef, ColumnOption, DataType as SqlDataType, ObjectName};
use crate::error::{self, Result};

/// Converts maybe fully-qualified table name (`<catalog>.<schema>.<table>` or `<table>` when
/// catalog and schema are default) to tuple.  
pub fn table_idents_to_full_name(
    obj_name: &ObjectName,
) -> Result<(Option<String>, Option<String>, String)> {
    match &obj_name.0[..] {
        [table] => Ok((None, None, table.value.clone())),
        [catalog, schema, table] => Ok((
            Some(catalog.value.clone()),
            Some(schema.value.clone()),
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

pub fn column_def_to_schema(column_def: &ColumnDef) -> Result<ColumnSchema> {
    let is_nullable = column_def
        .options
        .iter()
        .any(|o| matches!(o.option, ColumnOption::Null));
    Ok(ColumnSchema {
        name: column_def.name.value.clone(),
        data_type: sql_data_type_to_concrete_data_type(&column_def.data_type)?,
        is_nullable,
    })
}

fn sql_data_type_to_concrete_data_type(data_type: &SqlDataType) -> Result<ConcreteDataType> {
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
}
