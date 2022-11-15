// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use catalog::SchemaProviderRef;
use common_error::snafu::ensure;
use datatypes::prelude::ConcreteDataType;
use datatypes::vectors::VectorBuilder;
use snafu::{OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements;
use sql::statements::insert::Insert;
use table::requests::InsertRequest;

use crate::error::{self, Result};

// TODO(fys): Extract the common logic in datanode and frontend in the future.
#[allow(dead_code)]
pub(crate) fn insert_to_request(
    schema_provider: &SchemaProviderRef,
    stmt: Insert,
) -> Result<InsertRequest> {
    let columns = stmt.columns();
    let values = stmt.values().context(error::ParseSqlSnafu)?;
    let (catalog_name, schema_name, table_name) =
        stmt.full_table_name().context(error::ParseSqlSnafu)?;

    let table = schema_provider
        .table(&table_name)
        .context(error::CatalogSnafu)?
        .context(error::TableNotFoundSnafu {
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
            let column_schema = schema.column_schema_by_name(column_name).with_context(|| {
                error::ColumnNotFoundSnafu {
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

    for row in values {
        ensure!(
            row.len() == columns_num,
            error::ColumnValuesNumberMismatchSnafu {
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

    Ok(InsertRequest {
        catalog_name,
        schema_name,
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
    let value = statements::sql_value_to_value(column_name, data_type, sql_val)
        .context(error::ParseSqlSnafu)?;
    builder.push(&value);

    Ok(())
}
