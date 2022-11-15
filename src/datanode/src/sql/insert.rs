use catalog::SchemaProviderRef;
use common_query::Output;
use datatypes::prelude::{ConcreteDataType, VectorBuilder};
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements::insert::Insert;
use sql::statements::{self};
use table::engine::TableReference;
use table::requests::*;

use crate::error::{
    CatalogSnafu, ColumnNotFoundSnafu, ColumnValuesNumberMismatchSnafu, InsertSnafu, ParseSqlSnafu,
    ParseSqlValueSnafu, Result, TableNotFoundSnafu,
};
use crate::sql::{SqlHandler, SqlRequest};

impl SqlHandler {
    pub(crate) async fn insert(&self, req: InsertRequest) -> Result<Output> {
        // FIXME(dennis): table_ref is used in InsertSnafu and the req is consumed
        // in `insert`, so we have to clone catalog_name etc.
        let table_ref = TableReference {
            catalog: &req.catalog_name.to_string(),
            schema: &req.schema_name.to_string(),
            table: &req.table_name.to_string(),
        };

        let table = self.get_table(&table_ref)?;

        let affected_rows = table.insert(req).await.with_context(|_| InsertSnafu {
            table_name: table_ref.to_string(),
        })?;

        Ok(Output::AffectedRows(affected_rows))
    }

    pub(crate) fn insert_to_request(
        &self,
        schema_provider: SchemaProviderRef,
        stmt: Insert,
    ) -> Result<SqlRequest> {
        let columns = stmt.columns();
        let values = stmt.values().context(ParseSqlValueSnafu)?;
        let (catalog_name, schema_name, table_name) =
            stmt.full_table_name().context(ParseSqlSnafu)?;

        let table = schema_provider
            .table(&table_name)
            .context(CatalogSnafu)?
            .context(TableNotFoundSnafu {
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
            catalog_name,
            schema_name,
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
    let value = statements::sql_value_to_value(column_name, data_type, sql_val)
        .context(ParseSqlValueSnafu)?;
    builder.push(&value);

    Ok(())
}
