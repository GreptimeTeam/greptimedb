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

//! Implementation of `SHOW CREATE TABLE` statement.

use std::fmt::Display;

use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, SchemaRef, COMMENT_KEY};
use humantime::format_duration;
use snafu::ResultExt;
use sql::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, Expr, Ident, ObjectName, SqlOption, TableConstraint,
    Value as SqlValue,
};
use sql::dialect::GreptimeDbDialect;
use sql::parser::ParserContext;
use sql::statements::create::{CreateTable, TIME_INDEX};
use sql::statements::{self};
use table::metadata::{TableInfoRef, TableMeta};
use table::requests::FILE_TABLE_META_KEY;

use crate::error::{ConvertSqlTypeSnafu, ConvertSqlValueSnafu, Result, SqlSnafu};

#[inline]
fn number_value<T: Display>(n: T) -> SqlValue {
    SqlValue::Number(format!("{}", n), false)
}

#[inline]
fn string_value(s: impl Into<String>) -> SqlValue {
    SqlValue::SingleQuotedString(s.into())
}

#[inline]
fn sql_option(name: &str, value: SqlValue) -> SqlOption {
    SqlOption {
        name: name.into(),
        value,
    }
}

fn create_sql_options(table_meta: &TableMeta) -> Vec<SqlOption> {
    let table_opts = &table_meta.options;
    let mut options = Vec::with_capacity(4 + table_opts.extra_options.len());

    if !table_meta.region_numbers.is_empty() {
        options.push(sql_option(
            "regions",
            number_value(table_meta.region_numbers.len()),
        ));
    }

    if let Some(write_buffer_size) = table_opts.write_buffer_size {
        options.push(sql_option(
            "write_buffer_size",
            string_value(write_buffer_size.to_string()),
        ));
    }
    if let Some(ttl) = table_opts.ttl {
        options.push(sql_option(
            "ttl",
            string_value(format_duration(ttl).to_string()),
        ));
    }

    for (k, v) in table_opts
        .extra_options
        .iter()
        .filter(|(k, _)| k != &FILE_TABLE_META_KEY)
    {
        options.push(sql_option(k, string_value(v)));
    }

    options
}

#[inline]
fn column_option_def(option: ColumnOption) -> ColumnOptionDef {
    ColumnOptionDef { name: None, option }
}

fn create_column_def(column_schema: &ColumnSchema, quote_style: char) -> Result<ColumnDef> {
    let name = &column_schema.name;
    let mut options = Vec::with_capacity(2);

    if column_schema.is_nullable() {
        options.push(column_option_def(ColumnOption::Null));
    } else {
        options.push(column_option_def(ColumnOption::NotNull));
    }

    if let Some(c) = column_schema.default_constraint() {
        let expr = match c {
            ColumnDefaultConstraint::Value(v) => Expr::Value(
                statements::value_to_sql_value(v)
                    .with_context(|_| ConvertSqlValueSnafu { value: v.clone() })?,
            ),
            ColumnDefaultConstraint::Function(expr) => {
                ParserContext::parse_function(expr, &GreptimeDbDialect {}).context(SqlSnafu)?
            }
        };

        options.push(column_option_def(ColumnOption::Default(expr)));
    }

    if let Some(c) = column_schema.metadata().get(COMMENT_KEY) {
        options.push(column_option_def(ColumnOption::Comment(c.to_string())));
    }

    Ok(ColumnDef {
        name: Ident::with_quote(quote_style, name),
        data_type: statements::concrete_data_type_to_sql_data_type(&column_schema.data_type)
            .with_context(|_| ConvertSqlTypeSnafu {
                datatype: column_schema.data_type.clone(),
            })?,
        collation: None,
        options,
    })
}

fn create_table_constraints(
    schema: &SchemaRef,
    table_meta: &TableMeta,
    quote_style: char,
) -> Vec<TableConstraint> {
    let mut constraints = Vec::with_capacity(2);
    if let Some(timestamp_column) = schema.timestamp_column() {
        let column_name = &timestamp_column.name;
        constraints.push(TableConstraint::Unique {
            name: Some(TIME_INDEX.into()),
            columns: vec![Ident::with_quote(quote_style, column_name)],
            is_primary: false,
        });
    }
    if !table_meta.primary_key_indices.is_empty() {
        let columns = table_meta
            .row_key_column_names()
            .map(|name| Ident::with_quote(quote_style, name))
            .collect();
        constraints.push(TableConstraint::Unique {
            name: None,
            columns,
            is_primary: true,
        });
    }

    constraints
}

/// Create a CreateTable statement from table info.
pub fn create_table_stmt(table_info: &TableInfoRef, quote_style: char) -> Result<CreateTable> {
    let table_meta = &table_info.meta;
    let table_name = &table_info.name;
    let schema = &table_info.meta.schema;

    let columns = schema
        .column_schemas()
        .iter()
        .map(|c| create_column_def(c, quote_style))
        .collect::<Result<Vec<_>>>()?;

    let constraints = create_table_constraints(schema, table_meta, quote_style);

    Ok(CreateTable {
        if_not_exists: true,
        table_id: table_info.ident.table_id,
        name: ObjectName(vec![Ident::with_quote(quote_style, table_name)]),
        columns,
        engine: table_meta.engine.clone(),
        constraints,
        options: create_sql_options(table_meta),
        partitions: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::timestamp::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{Schema, SchemaRef};
    use table::metadata::*;
    use table::requests::{
        TableOptions, FILE_TABLE_FORMAT_KEY, FILE_TABLE_LOCATION_KEY, FILE_TABLE_META_KEY,
    };

    use super::*;

    #[test]
    fn test_show_create_table_sql() {
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

        let stmt = create_table_stmt(&info, '"').unwrap();

        let sql = format!("\n{}", stmt);
        assert_eq!(
            r#"
CREATE TABLE IF NOT EXISTS "system_metrics" (
  "id" INT UNSIGNED NULL,
  "host" STRING NULL,
  "cpu" DOUBLE NULL,
  "disk" FLOAT NULL,
  "ts" TIMESTAMP(3) NOT NULL DEFAULT current_timestamp(),
  TIME INDEX ("ts"),
  PRIMARY KEY ("id", "host")
)
ENGINE=mito
WITH(
  regions = 3
)"#,
            sql
        );
    }

    #[test]
    fn test_show_create_external_table_sql() {
        let schema = vec![
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
        ];
        let table_schema = SchemaRef::new(Schema::new(schema));
        let table_name = "system_metrics";
        let schema_name = "public".to_string();
        let catalog_name = "greptime".to_string();
        let mut options: TableOptions = Default::default();
        let _ = options
            .extra_options
            .insert(FILE_TABLE_LOCATION_KEY.to_string(), "foo.csv".to_string());
        let _ = options.extra_options.insert(
            FILE_TABLE_META_KEY.to_string(),
            "{{\"files\":[\"foo.csv\"]}}".to_string(),
        );
        let _ = options
            .extra_options
            .insert(FILE_TABLE_FORMAT_KEY.to_string(), "csv".to_string());
        let meta = TableMetaBuilder::default()
            .schema(table_schema)
            .primary_key_indices(vec![])
            .engine("file".to_string())
            .next_column_id(0)
            .options(options)
            .created_on(Default::default())
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

        let stmt = create_table_stmt(&info, '"').unwrap();

        let sql = format!("\n{}", stmt);
        assert_eq!(
            r#"
CREATE EXTERNAL TABLE IF NOT EXISTS "system_metrics" (
  "host" STRING NULL,
  "cpu" DOUBLE NULL,

)
ENGINE=file
WITH(
  format = 'csv',
  location = 'foo.csv'
)"#,
            sql
        );
    }
}
