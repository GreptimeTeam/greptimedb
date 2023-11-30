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

mod show_create_table;

use std::collections::HashMap;
use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_catalog::consts::{
    SEMANTIC_TYPE_FIELD, SEMANTIC_TYPE_PRIMARY_KEY, SEMANTIC_TYPE_TIME_INDEX,
};
use common_datasource::file_format::{infer_schemas, FileFormat, Format};
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::build_backend;
use common_datasource::util::find_dir_and_filename;
use common_query::Output;
use common_recordbatch::{RecordBatch, RecordBatches};
use common_time::Timestamp;
use datatypes::prelude::*;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, RawSchema, Schema};
use datatypes::vectors::{Helper, StringVector};
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use regex::Regex;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::create::Partitions;
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables};
use table::requests::{FILE_TABLE_LOCATION_KEY, FILE_TABLE_PATTERN_KEY};
use table::TableRef;

use crate::datafusion::execute_show_with_filter;
use crate::error::{self, Result};

const SCHEMAS_COLUMN: &str = "Schemas";
const TABLES_COLUMN: &str = "Tables";
const COLUMN_NAME_COLUMN: &str = "Column";
const COLUMN_TYPE_COLUMN: &str = "Type";
const COLUMN_KEY_COLUMN: &str = "Key";
const COLUMN_NULLABLE_COLUMN: &str = "Null";
const COLUMN_DEFAULT_COLUMN: &str = "Default";
const COLUMN_SEMANTIC_TYPE_COLUMN: &str = "Semantic Type";

const NULLABLE_YES: &str = "YES";
const NULLABLE_NO: &str = "NO";
const PRI_KEY: &str = "PRI";

const GREPTIME_TIMESTAMP: &str = "greptime_timestamp";

static DESCRIBE_TABLE_OUTPUT_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ColumnSchema::new(
            COLUMN_NAME_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            COLUMN_TYPE_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(COLUMN_KEY_COLUMN, ConcreteDataType::string_datatype(), true),
        ColumnSchema::new(
            COLUMN_NULLABLE_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            COLUMN_DEFAULT_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        ),
        ColumnSchema::new(
            COLUMN_SEMANTIC_TYPE_COLUMN,
            ConcreteDataType::string_datatype(),
            false,
        ),
    ]))
});

static SHOW_CREATE_TABLE_OUTPUT_SCHEMA: Lazy<Arc<Schema>> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("Table", ConcreteDataType::string_datatype(), false),
        ColumnSchema::new("Create Table", ConcreteDataType::string_datatype(), false),
    ]))
});

pub async fn show_databases(
    stmt: ShowDatabases,
    catalog_manager: CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let mut databases = catalog_manager
        .schema_names(query_ctx.current_catalog())
        .await
        .context(error::CatalogSnafu)?;

    // TODO(dennis): Specify the order of the results in catalog manager API
    databases.sort();

    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        SCHEMAS_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    match stmt.kind {
        ShowKind::All => {
            let databases = Arc::new(StringVector::from(databases)) as _;
            let records = RecordBatches::try_from_columns(schema, vec![databases])
                .context(error::CreateRecordBatchSnafu)?;
            Ok(Output::RecordBatches(records))
        }
        ShowKind::Where(filter) => {
            let columns = vec![Arc::new(StringVector::from(databases)) as _];
            let record_batch =
                RecordBatch::new(schema, columns).context(error::CreateRecordBatchSnafu)?;
            let result = execute_show_with_filter(record_batch, Some(filter)).await?;
            Ok(result)
        }
        ShowKind::Like(ident) => {
            let databases = Helper::like_utf8(databases, &ident.value)
                .context(error::VectorComputationSnafu)?;
            let records = RecordBatches::try_from_columns(schema, vec![databases])
                .context(error::CreateRecordBatchSnafu)?;
            Ok(Output::RecordBatches(records))
        }
    }
}

pub async fn show_tables(
    stmt: ShowTables,
    catalog_manager: CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let schema_name = if let Some(database) = stmt.database {
        database
    } else {
        query_ctx.current_schema().to_owned()
    };
    // TODO(sunng87): move this function into query_ctx
    let mut tables = catalog_manager
        .table_names(query_ctx.current_catalog(), &schema_name)
        .await
        .context(error::CatalogSnafu)?;

    // TODO(dennis): Specify the order of the results in schema provider API
    tables.sort();

    let table_types: Option<Arc<dyn Vector>> = {
        if stmt.full {
            Some(
                get_table_types(
                    &tables,
                    catalog_manager.clone(),
                    query_ctx.clone(),
                    &schema_name,
                )
                .await?,
            )
        } else {
            None
        }
    };

    let mut column_schema = vec![ColumnSchema::new(
        TABLES_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )];
    if table_types.is_some() {
        column_schema.push(ColumnSchema::new(
            "Table_type",
            ConcreteDataType::string_datatype(),
            false,
        ));
    }

    let schema = Arc::new(Schema::new(column_schema));

    match stmt.kind {
        ShowKind::All => {
            let tables = Arc::new(StringVector::from(tables)) as _;
            let mut columns = vec![tables];
            if let Some(table_types) = table_types {
                columns.push(table_types)
            }

            let records = RecordBatches::try_from_columns(schema, columns)
                .context(error::CreateRecordBatchSnafu)?;
            Ok(Output::RecordBatches(records))
        }
        ShowKind::Where(filter) => {
            let mut columns = vec![Arc::new(StringVector::from(tables)) as _];
            if let Some(table_types) = table_types {
                columns.push(table_types)
            }
            let record_batch =
                RecordBatch::new(schema, columns).context(error::CreateRecordBatchSnafu)?;
            let result = execute_show_with_filter(record_batch, Some(filter)).await?;
            Ok(result)
        }
        ShowKind::Like(ident) => {
            let (tables, filter) = Helper::like_utf8_filter(tables, &ident.value)
                .context(error::VectorComputationSnafu)?;
            let mut columns = vec![tables];

            if let Some(table_types) = table_types {
                let table_types = table_types
                    .filter(&filter)
                    .context(error::VectorComputationSnafu)?;
                columns.push(table_types)
            }

            let records = RecordBatches::try_from_columns(schema, columns)
                .context(error::CreateRecordBatchSnafu)?;
            Ok(Output::RecordBatches(records))
        }
    }
}

pub fn show_create_table(
    table: TableRef,
    partitions: Option<Partitions>,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let table_info = table.table_info();
    let table_name = &table_info.name;

    // Default to double quote and fallback to back quote
    let quote_style = if query_ctx.sql_dialect().is_delimited_identifier_start('"') {
        '"'
    } else if query_ctx.sql_dialect().is_delimited_identifier_start('\'') {
        '\''
    } else {
        '`'
    };

    let mut stmt = show_create_table::create_table_stmt(&table_info, quote_style)?;
    stmt.partitions = partitions.map(|mut p| {
        p.set_quote(quote_style);
        p
    });
    let sql = format!("{}", stmt);
    let columns = vec![
        Arc::new(StringVector::from(vec![table_name.clone()])) as _,
        Arc::new(StringVector::from(vec![sql])) as _,
    ];
    let records = RecordBatches::try_from_columns(SHOW_CREATE_TABLE_OUTPUT_SCHEMA.clone(), columns)
        .context(error::CreateRecordBatchSnafu)?;

    Ok(Output::RecordBatches(records))
}

pub fn describe_table(table: TableRef) -> Result<Output> {
    let table_info = table.table_info();
    let columns_schemas = table_info.meta.schema.column_schemas();
    let columns = vec![
        describe_column_names(columns_schemas),
        describe_column_types(columns_schemas),
        describe_column_keys(columns_schemas, &table_info.meta.primary_key_indices),
        describe_column_nullables(columns_schemas),
        describe_column_defaults(columns_schemas),
        describe_column_semantic_types(columns_schemas, &table_info.meta.primary_key_indices),
    ];
    let records = RecordBatches::try_from_columns(DESCRIBE_TABLE_OUTPUT_SCHEMA.clone(), columns)
        .context(error::CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(records))
}

fn describe_column_names(columns_schemas: &[ColumnSchema]) -> VectorRef {
    Arc::new(StringVector::from_iterator(
        columns_schemas.iter().map(|cs| cs.name.as_str()),
    ))
}

fn describe_column_types(columns_schemas: &[ColumnSchema]) -> VectorRef {
    Arc::new(StringVector::from(
        columns_schemas
            .iter()
            .map(|cs| cs.data_type.name())
            .collect::<Vec<_>>(),
    ))
}

fn describe_column_keys(
    columns_schemas: &[ColumnSchema],
    primary_key_indices: &[usize],
) -> VectorRef {
    Arc::new(StringVector::from_iterator(
        columns_schemas.iter().enumerate().map(|(i, cs)| {
            if cs.is_time_index() || primary_key_indices.contains(&i) {
                PRI_KEY
            } else {
                ""
            }
        }),
    ))
}

fn describe_column_nullables(columns_schemas: &[ColumnSchema]) -> VectorRef {
    Arc::new(StringVector::from_iterator(columns_schemas.iter().map(
        |cs| {
            if cs.is_nullable() {
                NULLABLE_YES
            } else {
                NULLABLE_NO
            }
        },
    )))
}

fn describe_column_defaults(columns_schemas: &[ColumnSchema]) -> VectorRef {
    Arc::new(StringVector::from(
        columns_schemas
            .iter()
            .map(|cs| {
                cs.default_constraint()
                    .map_or(String::from(""), |dc| dc.to_string())
            })
            .collect::<Vec<String>>(),
    ))
}

fn describe_column_semantic_types(
    columns_schemas: &[ColumnSchema],
    primary_key_indices: &[usize],
) -> VectorRef {
    Arc::new(StringVector::from_iterator(
        columns_schemas.iter().enumerate().map(|(i, cs)| {
            if primary_key_indices.contains(&i) {
                SEMANTIC_TYPE_PRIMARY_KEY
            } else if cs.is_time_index() {
                SEMANTIC_TYPE_TIME_INDEX
            } else {
                SEMANTIC_TYPE_FIELD
            }
        }),
    ))
}

// lists files in the frontend to reduce unnecessary scan requests repeated in each datanode.
pub async fn prepare_file_table_files(
    options: &HashMap<String, String>,
) -> Result<(ObjectStore, Vec<String>)> {
    let url = options
        .get(FILE_TABLE_LOCATION_KEY)
        .context(error::MissingRequiredFieldSnafu {
            name: FILE_TABLE_LOCATION_KEY,
        })?;

    let (dir, filename) = find_dir_and_filename(url);
    let source = if let Some(filename) = filename {
        Source::Filename(filename)
    } else {
        Source::Dir
    };
    let regex = options
        .get(FILE_TABLE_PATTERN_KEY)
        .map(|x| Regex::new(x))
        .transpose()
        .context(error::BuildRegexSnafu)?;
    let object_store = build_backend(url, options).context(error::BuildBackendSnafu)?;
    let lister = Lister::new(object_store.clone(), source, dir, regex);
    // If we scan files in a directory every time the database restarts,
    // then it might lead to a potential undefined behavior:
    // If a user adds a file with an incompatible schema to that directory,
    // it will make the external table unavailable.
    let files = lister
        .list()
        .await
        .context(error::ListObjectsSnafu)?
        .into_iter()
        .filter_map(|entry| {
            if entry.path().ends_with('/') {
                None
            } else {
                Some(entry.path().to_string())
            }
        })
        .collect::<Vec<_>>();
    Ok((object_store, files))
}

pub async fn infer_file_table_schema(
    object_store: &ObjectStore,
    files: &[String],
    options: &HashMap<String, String>,
) -> Result<RawSchema> {
    let format = parse_file_table_format(options)?;
    let merged = infer_schemas(object_store, files, format.as_ref())
        .await
        .context(error::InferSchemaSnafu)?;
    Ok(RawSchema::from(
        &Schema::try_from(merged).context(error::ConvertSchemaSnafu)?,
    ))
}

// Converts the file column schemas to table column schemas.
// Returns the column schemas and the time index column name.
//
// More specifically, this function will do the following:
// 1. Add a default time index column if there is no time index column
//    in the file column schemas, or
// 2. If the file column schemas contain a column with name conflicts with
//    the default time index column, it will replace the column schema
//    with the default one.
pub fn file_column_schemas_to_table(
    file_column_schemas: &[ColumnSchema],
) -> (Vec<ColumnSchema>, String) {
    let mut column_schemas = file_column_schemas.to_owned();
    if let Some(time_index_column) = column_schemas.iter().find(|c| c.is_time_index()) {
        let time_index = time_index_column.name.clone();
        return (column_schemas, time_index);
    }

    let timestamp_type = ConcreteDataType::timestamp_millisecond_datatype();
    let default_zero = Value::Timestamp(Timestamp::new_millisecond(0));
    let timestamp_column_schema = ColumnSchema::new(GREPTIME_TIMESTAMP, timestamp_type, false)
        .with_time_index(true)
        .with_default_constraint(Some(ColumnDefaultConstraint::Value(default_zero)))
        .unwrap();

    if let Some(column_schema) = column_schemas
        .iter_mut()
        .find(|column_schema| column_schema.name == GREPTIME_TIMESTAMP)
    {
        // Replace the column schema with the default one
        *column_schema = timestamp_column_schema;
    } else {
        column_schemas.push(timestamp_column_schema);
    }

    (column_schemas, GREPTIME_TIMESTAMP.to_string())
}

/// This function checks if the column schemas from a file can be matched with
/// the column schemas of a table.
///
/// More specifically, for each column seen in the table schema,
/// - If the same column does exist in the file schema, it checks if the data
/// type of the file column can be casted into the form of the table column.
/// - If the same column does not exist in the file schema, it checks if the
/// table column is nullable or has a default constraint.
pub fn check_file_to_table_schema_compatibility(
    file_column_schemas: &[ColumnSchema],
    table_column_schemas: &[ColumnSchema],
) -> Result<()> {
    let file_schemas_map = file_column_schemas
        .iter()
        .map(|s| (s.name.clone(), s))
        .collect::<HashMap<_, _>>();

    for table_column in table_column_schemas {
        if let Some(file_column) = file_schemas_map.get(&table_column.name) {
            // TODO(zhongzc): a temporary solution, we should use `can_cast_to` once it's ready.
            ensure!(
                file_column
                    .data_type
                    .can_arrow_type_cast_to(&table_column.data_type),
                error::ColumnSchemaIncompatibleSnafu {
                    column: table_column.name.clone(),
                    file_type: file_column.data_type.clone(),
                    table_type: table_column.data_type.clone(),
                }
            );
        } else {
            ensure!(
                table_column.is_nullable() || table_column.default_constraint().is_some(),
                error::ColumnSchemaNoDefaultSnafu {
                    column: table_column.name.clone(),
                }
            );
        }
    }

    Ok(())
}

fn parse_file_table_format(options: &HashMap<String, String>) -> Result<Box<dyn FileFormat>> {
    Ok(
        match Format::try_from(options).context(error::ParseFileFormatSnafu)? {
            Format::Csv(format) => Box::new(format),
            Format::Json(format) => Box::new(format),
            Format::Parquet(format) => Box::new(format),
            Format::Orc(format) => Box::new(format),
        },
    )
}

async fn get_table_types(
    tables: &[String],
    catalog_manager: CatalogManagerRef,
    query_ctx: QueryContextRef,
    schema_name: &str,
) -> Result<Arc<dyn Vector>> {
    let mut table_types = Vec::with_capacity(tables.len());
    for table_name in tables {
        if let Some(table) = catalog_manager
            .table(query_ctx.current_catalog(), schema_name, table_name)
            .await
            .context(error::CatalogSnafu)?
        {
            table_types.push(table.table_type().to_string());
        }
    }
    Ok(Arc::new(StringVector::from(table_types)) as _)
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_query::Output;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use common_time::timestamp::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{StringVector, TimestampMillisecondVector, UInt32Vector, VectorRef};
    use snafu::ResultExt;
    use table::test_util::MemTable;
    use table::TableRef;

    use crate::error;
    use crate::error::Result;
    use crate::sql::{
        describe_table, DESCRIBE_TABLE_OUTPUT_SCHEMA, NULLABLE_NO, NULLABLE_YES,
        SEMANTIC_TYPE_FIELD, SEMANTIC_TYPE_TIME_INDEX,
    };

    #[test]
    fn test_describe_table_multiple_columns() -> Result<()> {
        let table_name = "test_table";
        let schema = vec![
            ColumnSchema::new("t1", ConcreteDataType::uint32_datatype(), true),
            ColumnSchema::new(
                "t2",
                ConcreteDataType::timestamp_datatype(TimeUnit::Millisecond),
                false,
            )
            .with_default_constraint(Some(ColumnDefaultConstraint::Function(String::from(
                "current_timestamp()",
            ))))
            .unwrap()
            .with_time_index(true),
        ];
        let data = vec![
            Arc::new(UInt32Vector::from_slice([0])) as _,
            Arc::new(TimestampMillisecondVector::from_slice([0])) as _,
        ];
        let expected_columns = vec![
            Arc::new(StringVector::from(vec!["t1", "t2"])) as _,
            Arc::new(StringVector::from(vec!["UInt32", "TimestampMillisecond"])) as _,
            Arc::new(StringVector::from(vec!["", "PRI"])) as _,
            Arc::new(StringVector::from(vec![NULLABLE_YES, NULLABLE_NO])) as _,
            Arc::new(StringVector::from(vec!["", "current_timestamp()"])) as _,
            Arc::new(StringVector::from(vec![
                SEMANTIC_TYPE_FIELD,
                SEMANTIC_TYPE_TIME_INDEX,
            ])) as _,
        ];

        describe_table_test_by_schema(table_name, schema, data, expected_columns)
    }

    fn describe_table_test_by_schema(
        table_name: &str,
        schema: Vec<ColumnSchema>,
        data: Vec<VectorRef>,
        expected_columns: Vec<VectorRef>,
    ) -> Result<()> {
        let table_schema = SchemaRef::new(Schema::new(schema));
        let table = prepare_describe_table(table_name, table_schema, data);

        let expected =
            RecordBatches::try_from_columns(DESCRIBE_TABLE_OUTPUT_SCHEMA.clone(), expected_columns)
                .context(error::CreateRecordBatchSnafu)?;

        if let Output::RecordBatches(res) = describe_table(table)? {
            assert_eq!(res.take(), expected.take());
        } else {
            panic!("describe table must return record batch");
        }

        Ok(())
    }

    fn prepare_describe_table(
        table_name: &str,
        table_schema: SchemaRef,
        data: Vec<VectorRef>,
    ) -> TableRef {
        let record_batch = RecordBatch::new(table_schema, data).unwrap();
        MemTable::table(table_name, record_batch)
    }
}
