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
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema};
use itertools::Itertools;
use sql::statements;
use table::metadata::TableInfoRef;

const COMMA: &str = ",";
const COMMA_NEWLINE: &str = ",\n";

pub struct ShowCreateTableBuilder {
    table_info: TableInfoRef,
}

impl ShowCreateTableBuilder {
    pub fn new(table_info: TableInfoRef) -> Self {
        Self { table_info }
    }

    #[inline]
    fn quote_name(name: &str) -> String {
        format!("\"{name}\"")
    }

    fn new_create_table_sql(&self) -> String {
        format!(
            "CREATE TABLE {} (\n",
            Self::quote_name(&self.table_info.name)
        )
    }

    fn get_column_def_sql(column_schema: &ColumnSchema) -> String {
        let mut sql = Self::quote_name(&column_schema.name);
        sql.push_str(" ");
        // column data type
        let sql_type =
            statements::concrete_data_type_to_sql_data_type(&column_schema.data_type).unwrap();
        sql.push_str(&sql_type.to_string());
        // column options
        if column_schema.is_nullable() {
            sql.push_str(" NULL");
        } else {
            sql.push_str(" NOT NULL");
        }
        if let Some(c) = column_schema.default_constraint() {
            sql.push_str(" DEFAULT ");
            match c {
                ColumnDefaultConstraint::Value(v) => sql.push_str(&v.to_string()),
                ColumnDefaultConstraint::Function(exp) => sql.push_str(exp),
            }
        }

        sql
    }

    pub fn build(&self) -> String {
        let table_meta = &self.table_info.meta;
        let schema = &self.table_info.meta.schema;
        let columns_schemas = schema.column_schemas();

        let mut sql = self.new_create_table_sql();

        // columns
        let column_defs = columns_schemas
            .iter()
            .map(Self::get_column_def_sql)
            .join(COMMA_NEWLINE);
        sql.push_str(&column_defs);
        // time index
        if let Some(timestamp_column) = schema.timestamp_column() {
            sql.push_str(COMMA_NEWLINE);
            sql.push_str("TIME INDEX(");
            sql.push_str(&Self::quote_name(&timestamp_column.name));
            sql.push_str(")");
        }
        // primary keys
        if !table_meta.primary_key_indices.is_empty() {
            sql.push_str(COMMA_NEWLINE);
            sql.push_str("PRIMARY KEY(");
            sql.push_str(
                &table_meta
                    .row_key_column_names()
                    .map(|name| Self::quote_name(name))
                    .join(COMMA),
            );
            sql.push_str(")");
        }
        // engine
        sql.push_str(") ENGINE=");
        sql.push_str(&table_meta.engine);
        // table options
        let table_opts = &table_meta.options;
        sql.push_str("\nWITH (\n");
        sql.push_str("  ");
        sql.push_str(&format!("regions={}\n", table_meta.region_numbers.len()));
        if let Some(write_buffer_size) = table_opts.write_buffer_size {
            sql.push_str(&format!(",write_buffer_size={}\n", write_buffer_size));
        }
        if let Some(ttl) = table_opts.ttl {
            //sql.push_str(&format!(",ttl={}\n", ttl));
        }
        sql.push_str(")");

        sql
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::timestamp::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{Schema, SchemaRef};
    use table::metadata::*;

    use super::*;

    #[test]
    fn test_show_create_table_builder() {
        let schema = vec![
            ColumnSchema::new("id", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("disk", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond),
                false,
            )
            .with_default_constraint(Some(ColumnDefaultConstraint::Function(String::from(
                "current_timestamp()",
            ))))
            .unwrap()
            .with_time_index(true),
        ];
        let table_schema = SchemaRef::new(Schema::new(schema));
        let table_name = "system_metrics";
        let schema_name = "public".to_string();
        let catalog_name = "greptime".to_string();
        let regions = vec![0, 1, 2];

        let meta = TableMetaBuilder::default()
            .schema(table_schema)
            .primary_key_indices(vec![0, 1])
            .value_indices(vec![2, 3])
            .engine("mito".to_string())
            .next_column_id(0)
            .engine_options(Default::default())
            .options(Default::default())
            .created_on(Default::default())
            .region_numbers(regions)
            .build()
            .unwrap();

        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(1024)
                .table_version(0 as TableVersion)
                .name(table_name)
                .schema_name(schema_name)
                .catalog_name(catalog_name)
                .desc(None)
                .table_type(TableType::Base)
                .meta(meta)
                .build()
                .unwrap(),
        );

        let builder = ShowCreateTableBuilder::new(info);

        let sql = builder.build();

        println!("{}", sql);

        assert!(false);
    }
}
