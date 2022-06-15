//! sql handler
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
use sql::statements::statement::Statement;
use table::engine::{EngineContext, TableEngine};
use table::requests::*;

use crate::error::{
    ColumnNotFoundSnafu, ColumnTypeMistchSnafu, ColumnValuesNumberMismatchSnafu, GetTableSnafu,
    InsertSnafu, ParseSqlValueSnafu, Result, TableNotFoundSnafu,
};

pub enum SqlRequest {
    Insert(InsertRequest),
}

// Handler to execute SQL except querying.
pub struct SqlHandler<Engine: TableEngine> {
    table_engine: Engine,
}

impl<Engine: TableEngine> SqlHandler<Engine>
where
    <Engine as TableEngine>::Error: 'static,
{
    pub fn new(table_engine: Engine) -> Self {
        Self { table_engine }
    }

    pub async fn execute(&self, request: SqlRequest) -> Result<Output> {
        match request {
            SqlRequest::Insert(req) => {
                let table_name = &req.table_name.to_string();
                let table = self
                    .table_engine
                    .get_table(&EngineContext::default(), &req.table_name)
                    .map_err(|e| Box::new(e) as _)
                    .context(GetTableSnafu { table_name })?
                    .context(TableNotFoundSnafu { table_name })?;

                let affetced_rows = table
                    .insert(req)
                    .await
                    .context(InsertSnafu { table_name })?;

                Ok(Output::AffectedRows(affetced_rows))
            }
        }
    }

    // Cast sql statement into sql request
    pub fn statement_to_request(
        &self,
        schema_provider: SchemaProviderRef,
        statement: Statement,
    ) -> Result<SqlRequest> {
        match statement {
            Statement::Insert(stmt) => {
                let columns = stmt.columns();
                let values = stmt.values();
                //TODO(dennis): table name may be in the form of `catalog.schema.table`
                let table_name = stmt.table_name();

                ensure!(
                    columns.is_empty() || values.iter().all(|v| v.len() == columns.len()),
                    ColumnValuesNumberMismatchSnafu {
                        columns: columns.len(),
                        values: values.len(),
                    }
                );
                let table =
                    schema_provider
                        .table(&table_name)
                        .with_context(|| TableNotFoundSnafu {
                            table_name: table_name.clone(),
                        })?;
                let schema = table.schema();
                let columns_number = if columns.is_empty() {
                    schema.column_schemas().len()
                } else {
                    columns.len()
                };
                let rows_num = values.len();

                let mut columns_builders: Vec<(&String, &ConcreteDataType, VectorBuilder)> =
                    Vec::with_capacity(columns_number);

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
                        let column_schema = schema
                            .column_schema_by_name(column_name)
                            .with_context(|| ColumnNotFoundSnafu {
                                table_name: table_name.clone(),
                                column_name: column_name.to_string(),
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
                        row.len() == columns_number,
                        ColumnValuesNumberMismatchSnafu {
                            columns: columns_number,
                            values: row.len(),
                        }
                    );

                    for (i, sql_val) in row.iter().enumerate() {
                        let (column_name, data_type, builder) =
                            columns_builders.get_mut(i).expect("unreachable");
                        Self::add_row_to_vector(column_name, data_type, sql_val, builder)?;
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
            _ => unimplemented!(),
        }
    }

    fn add_row_to_vector(
        column_name: &str,
        data_type: &ConcreteDataType,
        sql_val: &SqlValue,
        builder: &mut VectorBuilder,
    ) -> Result<()> {
        let value = match sql_val {
            SqlValue::Number(n, _) => sql_number_to_value(data_type, n)?,
            SqlValue::Null => Value::Null,
            SqlValue::Boolean(b) => {
                ensure!(
                    data_type.is_boolean(),
                    ColumnTypeMistchSnafu {
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
                    ColumnTypeMistchSnafu {
                        column_name,
                        expect: data_type.clone(),
                        actual: ConcreteDataType::string_datatype(),
                    }
                );

                s.to_owned().into()
            }

            _ => todo!("Other sql value"),
        };
        builder.push(&value);
        Ok(())
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
