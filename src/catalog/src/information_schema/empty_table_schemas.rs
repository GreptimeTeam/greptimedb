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

use std::sync::Arc;

/// Return the schemas of tables which are not implemented.
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};

use crate::information_schema::table_names::*;

/// Find the schema by the table_name.
/// Safety: the user MUST ensure the table schema exists, panic otherwise.
pub fn get_schema(table_name: &'static str) -> SchemaRef {
    let column_schemas = match table_name {
        // https://dev.mysql.com/doc/refman/8.0/en/information-schema-column-privileges-table.html
        COLUMN_PRIVILEGES => string_columns(&[
            "GRANTEE",
            "TABLE_CATALOG",
            "TABLE_SCHEMA",
            "TABLE_NAME",
            "COLUMN_NAME",
            "PRIVILEGE_TYPE",
            "IS_GRANTABLE",
        ]),
        // https://dev.mysql.com/doc/refman/8.0/en/information-schema-column-statistics-table.html
        COLUMN_STATISTICS => string_columns(&[
            "SCHEMA_NAME",
            "TABLE_NAME",
            "COLUMN_NAME",
            // TODO(dennis): It must be a JSON type, but we don't support it yet
            "HISTOGRAM",
        ]),

        _ => unreachable!("Unknown table in information_schema: {}", table_name),
    };

    Arc::new(Schema::new(column_schemas))
}

fn string_columns(names: &[&'static str]) -> Vec<ColumnSchema> {
    names.iter().map(|name| string_column(name)).collect()
}

fn string_column(name: &str) -> ColumnSchema {
    ColumnSchema::new(
        str::to_lowercase(name),
        ConcreteDataType::string_datatype(),
        false,
    )
}

#[cfg(test)]
mod tests {}
