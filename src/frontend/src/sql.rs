use catalog::SchemaProviderRef;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorBuilder;
use sql::ast::Value as SqlValue;
use sql::statements;
use sql::statements::insert::Insert;
use table::requests::InsertRequest;

use crate::error::Result;

pub(crate) fn insert_to_request(
    schema_provider: &SchemaProviderRef,
    stmt: Insert,
) -> Result<InsertRequest> {
    let columns = stmt.columns();
    let values = stmt.values();
    let table_name = stmt.table_name();

    // TODO(fys): remove unwrap()
    let table = schema_provider.table(&table_name).unwrap().unwrap();
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
            // TODO(fys): remove unwrap()
            let column_schema = schema.column_schema_by_name(column_name).unwrap();
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
        assert!(row.len() == columns_num);

        for (sql_val, (column_name, data_type, builder)) in
            row.iter().zip(columns_builders.iter_mut())
        {
            add_row_to_vector(column_name, data_type, sql_val, builder)?;
        }
    }

    Ok(InsertRequest {
        table_name,
        columns_values: columns_builders
            .into_iter()
            .map(|(c, _, mut b)| (c.to_owned(), b.finish()))
            .collect(),
    })
}

fn add_row_to_vector(
    column_name: &str,
    data_type: &ConcreteDataType,
    sql_val: &SqlValue,
    builder: &mut VectorBuilder,
) -> Result<()> {
    // TODO(fys): remove unwrap()
    let value = statements::sql_value_to_value(column_name, data_type, sql_val).unwrap();
    builder.push(&value);
    Ok(())
}
