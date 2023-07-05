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

mod show;

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
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, RawSchema, Schema};
use datatypes::vectors::{Helper, StringVector};
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use regex::Regex;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::ColumnDef;
use sql::statements::column_def_to_schema;
use sql::statements::create::Partitions;
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables};
use table::requests::{IMMUTABLE_TABLE_LOCATION_KEY, IMMUTABLE_TABLE_PATTERN_KEY};
use table::TableRef;

use crate::datafusion::execute_show_with_filter;
use crate::error::{self, Result};

const SCHEMAS_COLUMN: &str = "Schemas";
const TABLES_COLUMN: &str = "Tables";
const COLUMN_NAME_COLUMN: &str = "Field";
const COLUMN_TYPE_COLUMN: &str = "Type";
const COLUMN_NULLABLE_COLUMN: &str = "Null";
const COLUMN_DEFAULT_COLUMN: &str = "Default";
const COLUMN_SEMANTIC_TYPE_COLUMN: &str = "Semantic Type";

const NULLABLE_YES: &str = "YES";
const NULLABLE_NO: &str = "NO";

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
    ensure!(
        matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
        error::UnsupportedExprSnafu {
            name: stmt.kind.to_string(),
        }
    );

    let mut databases = catalog_manager
        .schema_names(&query_ctx.current_catalog())
        .await
        .context(error::CatalogSnafu)?;

    // TODO(dennis): Specify the order of the results in catalog manager API
    databases.sort();

    let databases = if let ShowKind::Like(ident) = stmt.kind {
        Helper::like_utf8(databases, &ident.value).context(error::VectorComputationSnafu)?
    } else {
        Arc::new(StringVector::from(databases))
    };

    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        SCHEMAS_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    let records = RecordBatches::try_from_columns(schema, vec![databases])
        .context(error::CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(records))
}

pub async fn show_tables(
    stmt: ShowTables,
    catalog_manager: CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let schema = if let Some(database) = stmt.database {
        database
    } else {
        query_ctx.current_schema()
    };
    // TODO(sunng87): move this function into query_ctx
    let mut tables = catalog_manager
        .table_names(&query_ctx.current_catalog(), &schema)
        .await
        .context(error::CatalogSnafu)?;

    // TODO(dennis): Specify the order of the results in schema provider API
    tables.sort();
    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        TABLES_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    match stmt.kind {
        ShowKind::All => {
            let tables = Arc::new(StringVector::from(tables)) as _;
            let records = RecordBatches::try_from_columns(schema, vec![tables])
                .context(error::CreateRecordBatchSnafu)?;
            Ok(Output::RecordBatches(records))
        }
        ShowKind::Where(filter) => {
            let columns = vec![Arc::new(StringVector::from(tables)) as _];
            let record_batch =
                RecordBatch::new(schema, columns).context(error::CreateRecordBatchSnafu)?;
            let result = execute_show_with_filter(record_batch, Some(filter)).await?;
            Ok(result)
        }
        ShowKind::Like(ident) => {
            let tables =
                Helper::like_utf8(tables, &ident.value).context(error::VectorComputationSnafu)?;
            let records = RecordBatches::try_from_columns(schema, vec![tables])
                .context(error::CreateRecordBatchSnafu)?;
            Ok(Output::RecordBatches(records))
        }
    }
}

pub fn show_create_table(table: TableRef, partitions: Option<Partitions>) -> Result<Output> {
    let table_info = table.table_info();
    let table_name = &table_info.name;
    let mut stmt = show::create_table_stmt(&table_info)?;
    stmt.partitions = partitions;
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
    Arc::new(StringVector::from_iterator(
        columns_schemas.iter().map(|cs| cs.data_type.name()),
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
    Arc::new(StringVector::from(
        columns_schemas
            .iter()
            .enumerate()
            .map(|(i, cs)| {
                if primary_key_indices.contains(&i) {
                    String::from(SEMANTIC_TYPE_PRIMARY_KEY)
                } else if cs.is_time_index() {
                    String::from(SEMANTIC_TYPE_TIME_INDEX)
                } else {
                    String::from(SEMANTIC_TYPE_FIELD)
                }
            })
            .collect::<Vec<String>>(),
    ))
}

pub async fn prepare_immutable_file_table_files_and_schema(
    options: &HashMap<String, String>,
    columns: &Vec<ColumnDef>,
) -> Result<(Vec<String>, RawSchema)> {
    let (object_store, files) = prepare_immutable_file_table(options).await?;
    let schema = if !columns.is_empty() {
        let columns_schemas: Vec<_> = columns
            .iter()
            .map(|column| column_def_to_schema(column, false).context(error::ParseSqlSnafu))
            .collect::<Result<Vec<_>>>()?;
        RawSchema::new(columns_schemas)
    } else {
        let format = parse_immutable_file_table_format(options)?;
        infer_immutable_file_table_schema(&object_store, &*format, &files).await?
    };

    Ok((files, schema))
}

// lists files in the frontend to reduce unnecessary scan requests repeated in each datanode.
async fn prepare_immutable_file_table(
    options: &HashMap<String, String>,
) -> Result<(ObjectStore, Vec<String>)> {
    let url =
        options
            .get(IMMUTABLE_TABLE_LOCATION_KEY)
            .context(error::MissingRequiredFieldSnafu {
                name: IMMUTABLE_TABLE_LOCATION_KEY,
            })?;

    let (dir, filename) = find_dir_and_filename(url);
    let source = if let Some(filename) = filename {
        Source::Filename(filename)
    } else {
        Source::Dir
    };
    let regex = options
        .get(IMMUTABLE_TABLE_PATTERN_KEY)
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

fn parse_immutable_file_table_format(
    options: &HashMap<String, String>,
) -> Result<Box<dyn FileFormat>> {
    Ok(
        match Format::try_from(options).context(error::ParseFileFormatSnafu)? {
            Format::Csv(format) => Box::new(format),
            Format::Json(format) => Box::new(format),
            Format::Parquet(format) => Box::new(format),
            Format::Orc(format) => Box::new(format),
        },
    )
}

async fn infer_immutable_file_table_schema(
    object_store: &ObjectStore,
    file_format: &dyn FileFormat,
    files: &[String],
) -> Result<RawSchema> {
    let merged = infer_schemas(object_store, files, file_format)
        .await
        .context(error::InferSchemaSnafu)?;
    Ok(RawSchema::from(
        &Schema::try_from(merged).context(error::ConvertSchemaSnafu)?,
    ))
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
        Arc::new(MemTable::new(table_name, record_batch))
    }
}
