use std::str::FromStr;

use datatypes::prelude::ConcreteDataType;
use datatypes::prelude::VectorBuilder;
use datatypes::value::Value;
use query::catalog::schema::SchemaProviderRef;
use query::query_engine::Output;
use snafu::ensure;
use snafu::OptionExt;
use snafu::ResultExt;
use sql::ast::Value as SqlValue;
use sql::statements::insert::Insert;
use table::engine::TableEngine;
use table::requests::*;

use crate::error::{
    ColumnNotFoundSnafu, ColumnTypeMismatchSnafu, ColumnValuesNumberMismatchSnafu, InsertSnafu,
    ParseSqlValueSnafu, Result, TableNotFoundSnafu,
};
use crate::sql::{SqlHandler, SqlRequest};

impl<Engine: TableEngine> SqlHandler<Engine> {
    pub(crate) async fn insert(&self, req: InsertRequest) -> Result<Output> {
        let table_name = &req.table_name.to_string();
        let table = self.get_table(table_name)?;

        let affected_rows = table
            .insert(req)
            .await
            .context(InsertSnafu { table_name })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub(crate) fn insert_to_request(
        &self,
        schema_provider: SchemaProviderRef,
        stmt: Insert,
    ) -> Result<SqlRequest> {
        let columns = stmt.columns();
        let values = stmt.values();
        //TODO(dennis): table name may be in the form of `catalog.schema.table`,
        //   but we don't process it right now.
        let table_name = stmt.table_name();

        let table = schema_provider
            .table(&table_name)
            .with_context(|| TableNotFoundSnafu {
                table_name: &table_name,
            })?;
        let schema = table.schema();
        let columns_num = if columns.is_empty() {
            schema.column_schemas().len()
        } else {
            columns.len()
        };
        let rows_num = values.len();

        let mut columns_builders: Vec<(&String, &ConcreteDataType, VectorBuilder)> =
            Vec::with_capacity(columns_num);

        if columns.is_empty() {
            for column_schema in schema.column_schemas() {
                let data_type = &column_schema.data_type;
                columns_builders.push((
                    &column_schema.name,
                    data_type,
                    VectorBuilder::with_capacity(data_type.clone(), rows_num),
                ));
            }
        } else {
            for column_name in columns {
                let column_schema =
                    schema.column_schema_by_name(column_name).with_context(|| {
                        ColumnNotFoundSnafu {
                            table_name: &table_name,
                            column_name: column_name.to_string(),
                        }
                    })?;
                let data_type = &column_schema.data_type;
                columns_builders.push((
                    column_name,
                    data_type,
                    VectorBuilder::with_capacity(data_type.clone(), rows_num),
                ));
            }
        }

        // Convert rows into columns
        for row in values {
            ensure!(
                row.len() == columns_num,
                ColumnValuesNumberMismatchSnafu {
                    columns: columns_num,
                    values: row.len(),
                }
            );

            for (sql_val, (column_name, data_type, builder)) in
                row.iter().zip(columns_builders.iter_mut())
            {
                add_row_to_vector(column_name, data_type, sql_val, builder)?;
            }
        }

        Ok(SqlRequest::Insert(InsertRequest {
            table_name,
            columns_values: columns_builders
                .into_iter()
                .map(|(c, _, mut b)| (c.to_owned(), b.finish()))
                .collect(),
        }))
    }
}

fn add_row_to_vector(
    column_name: &str,
    data_type: &ConcreteDataType,
    sql_val: &SqlValue,
    builder: &mut VectorBuilder,
) -> Result<()> {
    let value = parse_sql_value(column_name, data_type, sql_val)?;
    builder.push(&value);

    Ok(())
}

fn parse_sql_value(
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
            ensure!(
                data_type.is_string(),
                ColumnTypeMismatchSnafu {
                    column_name,
                    expect: data_type.clone(),
                    actual: ConcreteDataType::string_datatype(),
                }
            );

            s.to_owned().into()
        }

        _ => todo!("Other sql value"),
    })
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

fn sql_number_to_value(data_type: &ConcreteDataType, n: &str) -> Result<Value> {
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
        (Float32, f32)
    )
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

#[cfg(test)]
mod tests {
    use datatypes::value::OrderedFloat;

    use super::*;

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
    fn test_parse_sql_value() {
        let sql_val = SqlValue::Null;
        assert_eq!(
            Value::Null,
            parse_sql_value("a", &ConcreteDataType::float64_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Boolean(true);
        assert_eq!(
            Value::Boolean(true),
            parse_sql_value("a", &ConcreteDataType::boolean_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        assert_eq!(
            Value::Float64(OrderedFloat(3.0)),
            parse_sql_value("a", &ConcreteDataType::float64_datatype(), &sql_val).unwrap()
        );

        let sql_val = SqlValue::Number("3.0".to_string(), false);
        let v = parse_sql_value("a", &ConcreteDataType::boolean_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{:?}", v)
            .contains("Fail to parse number 3.0, invalid column type: Boolean(BooleanType)"));

        let sql_val = SqlValue::Boolean(true);
        let v = parse_sql_value("a", &ConcreteDataType::float64_datatype(), &sql_val);
        assert!(v.is_err());
        assert!(format!("{:?}", v).contains(
            "column_name: \"a\", expect: Float64(Float64), actual: Boolean(BooleanType)"
        ));
    }
}
