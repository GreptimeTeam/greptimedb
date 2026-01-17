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

use std::collections::HashMap;

use arrow_schema::extension::ExtensionType;
use common_meta::SchemaOptions;
use datatypes::extension::json::JsonExtensionType;
use datatypes::schema::{
    COLUMN_FULLTEXT_OPT_KEY_ANALYZER, COLUMN_FULLTEXT_OPT_KEY_BACKEND,
    COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE, COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE,
    COLUMN_FULLTEXT_OPT_KEY_GRANULARITY, COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE,
    COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY, COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE,
    COLUMN_VECTOR_INDEX_OPT_KEY_CONNECTIVITY, COLUMN_VECTOR_INDEX_OPT_KEY_ENGINE,
    COLUMN_VECTOR_INDEX_OPT_KEY_EXPANSION_ADD, COLUMN_VECTOR_INDEX_OPT_KEY_EXPANSION_SEARCH,
    COLUMN_VECTOR_INDEX_OPT_KEY_METRIC, COMMENT_KEY, ColumnDefaultConstraint, ColumnSchema,
    FulltextBackend, SchemaRef,
};
use snafu::ResultExt;
use sql::ast::{ColumnDef, ColumnOption, ColumnOptionDef, Expr, Ident, ObjectName};
use sql::dialect::GreptimeDbDialect;
use sql::parser::ParserContext;
use sql::statements::create::{Column, ColumnExtensions, CreateTable, TableConstraint};
use sql::statements::{self, OptionMap};
use store_api::metric_engine_consts::{is_metric_engine, is_metric_engine_internal_column};
use table::metadata::{TableInfoRef, TableMeta};
use table::requests::{
    COMMENT_KEY as TABLE_COMMENT_KEY, FILE_TABLE_META_KEY, TTL_KEY, WRITE_BUFFER_SIZE_KEY,
};

use crate::error::{
    ConvertSqlTypeSnafu, ConvertSqlValueSnafu, GetFulltextOptionsSnafu,
    GetSkippingIndexOptionsSnafu, GetVectorIndexOptionsSnafu, Result, SqlSnafu,
};

/// Generates CREATE TABLE options from given table metadata and schema-level options.
fn create_sql_options(table_meta: &TableMeta, schema_options: Option<SchemaOptions>) -> OptionMap {
    let table_opts = &table_meta.options;
    let mut options = OptionMap::default();
    if let Some(write_buffer_size) = table_opts.write_buffer_size {
        options.insert(
            WRITE_BUFFER_SIZE_KEY.to_string(),
            write_buffer_size.to_string(),
        );
    }
    if let Some(ttl) = table_opts.ttl.map(|t| t.to_string()) {
        options.insert(TTL_KEY.to_string(), ttl);
    } else if let Some(database_ttl) = schema_options
        .as_ref()
        .and_then(|o| o.ttl)
        .map(|ttl| ttl.to_string())
    {
        options.insert(TTL_KEY.to_string(), database_ttl);
    };

    for (k, v) in table_opts
        .extra_options
        .iter()
        .filter(|(k, _)| k != &FILE_TABLE_META_KEY)
    {
        options.insert(k.clone(), v.clone());
    }
    options
}

#[inline]
fn column_option_def(option: ColumnOption) -> ColumnOptionDef {
    ColumnOptionDef { name: None, option }
}

fn create_column(column_schema: &ColumnSchema, quote_style: char) -> Result<Column> {
    let name = &column_schema.name;
    let mut options = Vec::with_capacity(2);
    let mut extensions = ColumnExtensions::default();

    if column_schema.is_nullable() {
        options.push(column_option_def(ColumnOption::Null));
    } else {
        options.push(column_option_def(ColumnOption::NotNull));
    }

    if let Some(c) = column_schema.default_constraint() {
        let expr = match c {
            ColumnDefaultConstraint::Value(v) => Expr::Value(
                statements::value_to_sql_value(v)
                    .with_context(|_| ConvertSqlValueSnafu { value: v.clone() })?
                    .into(),
            ),
            ColumnDefaultConstraint::Function(expr) => {
                ParserContext::parse_function(expr, &GreptimeDbDialect {}).context(SqlSnafu)?
            }
        };

        options.push(column_option_def(ColumnOption::Default(expr)));
    }

    if let Some(c) = column_schema.metadata().get(COMMENT_KEY) {
        options.push(column_option_def(ColumnOption::Comment(c.clone())));
    }

    if let Some(opt) = column_schema
        .fulltext_options()
        .context(GetFulltextOptionsSnafu)?
        && opt.enable
    {
        let mut map = HashMap::from([
            (
                COLUMN_FULLTEXT_OPT_KEY_ANALYZER.to_string(),
                opt.analyzer.to_string(),
            ),
            (
                COLUMN_FULLTEXT_OPT_KEY_CASE_SENSITIVE.to_string(),
                opt.case_sensitive.to_string(),
            ),
            (
                COLUMN_FULLTEXT_OPT_KEY_BACKEND.to_string(),
                opt.backend.to_string(),
            ),
        ]);
        if opt.backend == FulltextBackend::Bloom {
            map.insert(
                COLUMN_FULLTEXT_OPT_KEY_GRANULARITY.to_string(),
                opt.granularity.to_string(),
            );
            map.insert(
                COLUMN_FULLTEXT_OPT_KEY_FALSE_POSITIVE_RATE.to_string(),
                opt.false_positive_rate().to_string(),
            );
        }
        extensions.fulltext_index_options = Some(map.into());
    }

    if let Some(opt) = column_schema
        .skipping_index_options()
        .context(GetSkippingIndexOptionsSnafu)?
    {
        let map = HashMap::from([
            (
                COLUMN_SKIPPING_INDEX_OPT_KEY_GRANULARITY.to_string(),
                opt.granularity.to_string(),
            ),
            (
                COLUMN_SKIPPING_INDEX_OPT_KEY_FALSE_POSITIVE_RATE.to_string(),
                opt.false_positive_rate().to_string(),
            ),
            (
                COLUMN_SKIPPING_INDEX_OPT_KEY_TYPE.to_string(),
                opt.index_type.to_string(),
            ),
        ]);
        extensions.skipping_index_options = Some(map.into());
    }

    if let Some(opt) = column_schema
        .vector_index_options()
        .context(GetVectorIndexOptionsSnafu)?
    {
        let map = HashMap::from([
            (
                COLUMN_VECTOR_INDEX_OPT_KEY_ENGINE.to_string(),
                opt.engine.to_string(),
            ),
            (
                COLUMN_VECTOR_INDEX_OPT_KEY_METRIC.to_string(),
                opt.metric.to_string(),
            ),
            (
                COLUMN_VECTOR_INDEX_OPT_KEY_CONNECTIVITY.to_string(),
                opt.connectivity.to_string(),
            ),
            (
                COLUMN_VECTOR_INDEX_OPT_KEY_EXPANSION_ADD.to_string(),
                opt.expansion_add.to_string(),
            ),
            (
                COLUMN_VECTOR_INDEX_OPT_KEY_EXPANSION_SEARCH.to_string(),
                opt.expansion_search.to_string(),
            ),
        ]);
        extensions.vector_index_options = Some(map.into());
    }

    if column_schema.is_inverted_indexed() {
        extensions.inverted_index_options = Some(HashMap::new().into());
    }

    if let Some(json_extension) = column_schema.extension_type::<JsonExtensionType>()? {
        let settings = json_extension
            .metadata()
            .json_structure_settings
            .clone()
            .unwrap_or_default();
        extensions.set_json_structure_settings(settings);
    }

    Ok(Column {
        column_def: ColumnDef {
            name: Ident::with_quote(quote_style, name),
            data_type: statements::concrete_data_type_to_sql_data_type(&column_schema.data_type)
                .with_context(|_| ConvertSqlTypeSnafu {
                    datatype: column_schema.data_type.clone(),
                })?,
            options,
        },
        extensions,
    })
}

/// Returns the primary key columns for `SHOW CREATE TABLE` statement.
///
/// For metric engine, it will only return the primary key columns that are not internal columns.
fn primary_key_columns_for_show_create<'a>(
    table_meta: &'a TableMeta,
    engine: &str,
) -> Vec<&'a String> {
    let is_metric_engine = is_metric_engine(engine);
    if is_metric_engine {
        table_meta
            .row_key_column_names()
            .filter(|name| !is_metric_engine_internal_column(name))
            .collect()
    } else {
        table_meta.row_key_column_names().collect()
    }
}

fn create_table_constraints(
    engine: &str,
    schema: &SchemaRef,
    table_meta: &TableMeta,
    quote_style: char,
) -> Vec<TableConstraint> {
    let mut constraints = Vec::with_capacity(2);
    if let Some(timestamp_column) = schema.timestamp_column() {
        let column_name = &timestamp_column.name;
        constraints.push(TableConstraint::TimeIndex {
            column: Ident::with_quote(quote_style, column_name),
        });
    }
    let primary_key_columns = primary_key_columns_for_show_create(table_meta, engine);
    if !primary_key_columns.is_empty() {
        let columns = primary_key_columns
            .into_iter()
            .map(|name| Ident::with_quote(quote_style, name))
            .collect();
        constraints.push(TableConstraint::PrimaryKey { columns });
    }

    constraints
}

/// Create a CreateTable statement from table info.
pub fn create_table_stmt(
    table_info: &TableInfoRef,
    schema_options: Option<SchemaOptions>,
    quote_style: char,
) -> Result<CreateTable> {
    let table_meta = &table_info.meta;
    let table_name = &table_info.name;
    let schema = &table_info.meta.schema;
    let is_metric_engine = is_metric_engine(&table_meta.engine);
    let columns = schema
        .column_schemas()
        .iter()
        .filter_map(|c| {
            if is_metric_engine && is_metric_engine_internal_column(&c.name) {
                None
            } else {
                Some(create_column(c, quote_style))
            }
        })
        .collect::<Result<Vec<_>>>()?;

    let constraints = create_table_constraints(&table_meta.engine, schema, table_meta, quote_style);

    let mut options = create_sql_options(table_meta, schema_options);
    if let Some(comment) = &table_info.desc
        && options.get(TABLE_COMMENT_KEY).is_none()
    {
        options.insert(format!("'{TABLE_COMMENT_KEY}'"), comment.clone());
    }

    Ok(CreateTable {
        if_not_exists: true,
        table_id: table_info.ident.table_id,
        name: ObjectName::from(vec![Ident::with_quote(quote_style, table_name)]),
        columns,
        engine: table_meta.engine.clone(),
        constraints,
        options,
        partitions: None,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use common_time::timestamp::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{
        FulltextOptions, Schema, SchemaRef, SkippingIndexOptions, VectorIndexOptions,
    };
    use store_api::metric_engine_consts::DATA_SCHEMA_TSID_COLUMN_NAME;
    use table::metadata::*;
    use table::requests::{
        FILE_TABLE_FORMAT_KEY, FILE_TABLE_LOCATION_KEY, FILE_TABLE_META_KEY, TableOptions,
    };

    use super::*;

    #[test]
    fn test_show_create_table_sql() {
        let schema = vec![
            ColumnSchema::new("id", ConcreteDataType::uint32_datatype(), true)
                .with_skipping_options(SkippingIndexOptions {
                    granularity: 4096,
                    ..Default::default()
                })
                .unwrap(),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true)
                .with_inverted_index(true),
            ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new("disk", ConcreteDataType::float32_datatype(), true),
            ColumnSchema::new("msg", ConcreteDataType::string_datatype(), true)
                .with_fulltext_options(FulltextOptions {
                    enable: true,
                    ..Default::default()
                })
                .unwrap(),
            ColumnSchema::new("embedding", ConcreteDataType::vector_datatype(4), true)
                .with_vector_index_options(&VectorIndexOptions::default())
                .unwrap(),
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

        let mut options = table::requests::TableOptions {
            ttl: Some(Duration::from_secs(30).into()),
            ..Default::default()
        };

        let _ = options
            .extra_options
            .insert("compaction.type".to_string(), "twcs".to_string());

        let meta = TableMetaBuilder::empty()
            .schema(table_schema)
            .primary_key_indices(vec![0, 1])
            .value_indices(vec![2, 3])
            .engine("mito".to_string())
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

        let stmt = create_table_stmt(&info, None, '"').unwrap();

        let sql = format!("\n{}", stmt);
        assert_eq!(
            r#"
CREATE TABLE IF NOT EXISTS "system_metrics" (
  "id" INT UNSIGNED NULL SKIPPING INDEX WITH(false_positive_rate = '0.01', granularity = '4096', type = 'BLOOM'),
  "host" STRING NULL INVERTED INDEX,
  "cpu" DOUBLE NULL,
  "disk" FLOAT NULL,
  "msg" STRING NULL FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false', false_positive_rate = '0.01', granularity = '10240'),
  "embedding" VECTOR(4) NULL VECTOR INDEX WITH(connectivity = '16', engine = 'usearch', expansion_add = '128', expansion_search = '64', metric = 'l2sq'),
  "ts" TIMESTAMP(3) NOT NULL DEFAULT current_timestamp(),
  TIME INDEX ("ts"),
  PRIMARY KEY ("id", "host")
)
ENGINE=mito
WITH(
  'compaction.type' = 'twcs',
  ttl = '30s'
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
        let meta = TableMetaBuilder::empty()
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

        let stmt = create_table_stmt(&info, None, '"').unwrap();

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

    #[test]
    fn test_show_create_metric_table_empty_primary_key_is_omitted() {
        let schema = vec![
            ColumnSchema::new(
                "greptime_timestamp",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("greptime_value", ConcreteDataType::float64_datatype(), true),
            ColumnSchema::new(
                DATA_SCHEMA_TSID_COLUMN_NAME,
                ConcreteDataType::uint64_datatype(),
                false,
            ),
        ];
        let table_schema = SchemaRef::new(Schema::new(schema));
        let meta = TableMetaBuilder::empty()
            .schema(table_schema)
            .primary_key_indices(vec![2])
            .value_indices(vec![0, 1])
            .engine("metric".to_string())
            .next_column_id(0)
            .options(Default::default())
            .created_on(Default::default())
            .build()
            .unwrap();

        let info = Arc::new(
            TableInfoBuilder::default()
                .table_id(1024)
                .table_version(0 as TableVersion)
                .name("test_metric_table")
                .schema_name("public".to_string())
                .catalog_name("greptime".to_string())
                .desc(None)
                .table_type(TableType::Base)
                .meta(meta)
                .build()
                .unwrap(),
        );

        let stmt = create_table_stmt(&info, None, '"').unwrap();
        let sql = format!("\n{}", stmt);
        assert!(!sql.contains("PRIMARY KEY"));
    }
}
