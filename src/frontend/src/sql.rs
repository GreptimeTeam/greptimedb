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

use common_error::snafu::ensure;
use datatypes::data_type::DataType;
use datatypes::prelude::MutableVector;
use datatypes::schema::ColumnSchema;
use snafu::{OptionExt, ResultExt};
use sql::ast::Value as SqlValue;
use sql::statements;
use sql::statements::insert::Insert;
use table::requests::InsertRequest;
use table::TableRef;

use crate::error::{self, Result};

const DEFAULT_PLACEHOLDER_VALUE: &str = "default";

// TODO(fys): Extract the common logic in datanode and frontend in the future.
#[allow(dead_code)]
pub(crate) fn insert_to_request(table: &TableRef, stmt: Insert) -> Result<InsertRequest> {
    let columns = stmt.columns();
    let values = stmt.values().context(error::ParseSqlSnafu)?;
    let (catalog_name, schema_name, table_name) =
        stmt.full_table_name().context(error::ParseSqlSnafu)?;

    let schema = table.schema();
    let columns_num = if columns.is_empty() {
        schema.column_schemas().len()
    } else {
        columns.len()
    };
    let rows_num = values.len();

    let mut columns_builders: Vec<(&ColumnSchema, Box<dyn MutableVector>)> =
        Vec::with_capacity(columns_num);

    if columns.is_empty() {
        for column_schema in schema.column_schemas() {
            let data_type = &column_schema.data_type;
            columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
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
            columns_builders.push((column_schema, data_type.create_mutable_vector(rows_num)));
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

        for (sql_val, (column_schema, builder)) in row.iter().zip(columns_builders.iter_mut()) {
            add_row_to_vector(column_schema, sql_val, builder)?;
        }
    }

    Ok(InsertRequest {
        catalog_name,
        schema_name,
        table_name,
        columns_values: columns_builders
            .into_iter()
            .map(|(cs, mut b)| (cs.name.to_string(), b.to_vector()))
            .collect(),
    })
}

fn add_row_to_vector(
    column_schema: &ColumnSchema,
    sql_val: &SqlValue,
    builder: &mut Box<dyn MutableVector>,
) -> Result<()> {
    let value = if replace_default(sql_val) {
        column_schema
            .create_default()
            .context(error::ColumnDefaultValueSnafu {
                column: column_schema.name.to_string(),
            })?
            .context(error::ColumnNoneDefaultValueSnafu {
                column: column_schema.name.to_string(),
            })?
    } else {
        statements::sql_value_to_value(&column_schema.name, &column_schema.data_type, sql_val)
            .context(error::ParseSqlSnafu)?
    };
    builder.push_value_ref(value.as_value_ref()).unwrap();
    Ok(())
}

fn replace_default(sql_val: &SqlValue) -> bool {
    matches!(sql_val, SqlValue::Placeholder(s) if s.to_lowercase() == DEFAULT_PLACEHOLDER_VALUE)
}
