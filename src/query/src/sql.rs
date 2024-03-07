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

use catalog::information_schema::{schemata, tables, SCHEMATA, TABLES};
use catalog::CatalogManagerRef;
use common_catalog::consts::{
    INFORMATION_SCHEMA_NAME, SEMANTIC_TYPE_FIELD, SEMANTIC_TYPE_PRIMARY_KEY,
    SEMANTIC_TYPE_TIME_INDEX,
};
use common_catalog::format_full_table_name;
use common_datasource::file_format::{infer_schemas, FileFormat, Format};
use common_datasource::lister::{Lister, Source};
use common_datasource::object_store::build_backend;
use common_datasource::util::find_dir_and_filename;
use common_query::prelude::GREPTIME_TIMESTAMP;
use common_query::Output;
use common_recordbatch::adapter::RecordBatchStreamAdapter;
use common_recordbatch::RecordBatches;
use common_time::timezone::get_timezone;
use common_time::Timestamp;
use datafusion::prelude::SessionContext;
use datafusion_expr::{col, lit, Expr};
use datatypes::prelude::*;
use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, RawSchema, Schema};
use datatypes::vectors::StringVector;
use object_store::ObjectStore;
use once_cell::sync::Lazy;
use regex::Regex;
use session::context::QueryContextRef;
pub use show_create_table::create_table_stmt;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::create::Partitions;
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables, ShowVariables};
use table::requests::{FILE_TABLE_LOCATION_KEY, FILE_TABLE_PATTERN_KEY};
use table::TableRef;

use crate::dataframe::DataFrame;
use crate::error::{self, Result, UnsupportedVariableSnafu};
use crate::planner::DfLogicalPlanner;
use crate::QueryEngineRef;

const SCHEMAS_COLUMN: &str = "Database";
const TABLES_COLUMN: &str = "Tables";
const TABLE_TYPE_COLUMN: &str = "Table_type";
const COLUMN_NAME_COLUMN: &str = "Column";
const COLUMN_TYPE_COLUMN: &str = "Type";
const COLUMN_KEY_COLUMN: &str = "Key";
const COLUMN_NULLABLE_COLUMN: &str = "Null";
const COLUMN_DEFAULT_COLUMN: &str = "Default";
const COLUMN_SEMANTIC_TYPE_COLUMN: &str = "Semantic Type";

const NULLABLE_YES: &str = "YES";
const NULLABLE_NO: &str = "NO";
const PRI_KEY: &str = "PRI";

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
    query_engine: &QueryEngineRef,
    catalog_manager: &CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let projects = vec![(schemata::SCHEMA_NAME, SCHEMAS_COLUMN)];

    let filters = vec![col(schemata::CATALOG_NAME).eq(lit(query_ctx.current_catalog()))];
    let like_field = Some(schemata::SCHEMA_NAME);
    let sort = vec![col(schemata::SCHEMA_NAME).sort(true, true)];

    query_from_information_schema_table(
        query_engine,
        catalog_manager,
        query_ctx,
        SCHEMATA,
        projects,
        filters,
        like_field,
        sort,
        stmt.kind,
    )
    .await
}

/// Cast a `show` statement execution into a query from tables in  `information_schema`.
/// - `table_name`: the table name in `information_schema`,
/// - `projects`: query projection, a list of `(column, renamed_column)`,
/// - `filters`: filter expressions for query,
/// - `like_field`: the field to filter by the predicate `ShowKind::Like`,
/// - `sort`: sort the results by the specified sorting expressions,
/// - `kind`: the show kind
#[allow(clippy::too_many_arguments)]
async fn query_from_information_schema_table(
    query_engine: &QueryEngineRef,
    catalog_manager: &CatalogManagerRef,
    query_ctx: QueryContextRef,
    table_name: &str,
    projects: Vec<(&str, &str)>,
    filters: Vec<Expr>,
    like_field: Option<&str>,
    sort: Vec<Expr>,
    kind: ShowKind,
) -> Result<Output> {
    let table = catalog_manager
        .table(
            query_ctx.current_catalog(),
            INFORMATION_SCHEMA_NAME,
            table_name,
        )
        .await
        .context(error::CatalogSnafu)?
        .with_context(|| error::TableNotFoundSnafu {
            table: format_full_table_name(
                query_ctx.current_catalog(),
                INFORMATION_SCHEMA_NAME,
                table_name,
            ),
        })?;

    let DataFrame::DataFusion(dataframe) = query_engine.read_table(table)?;

    // Apply filters
    let dataframe = filters.into_iter().try_fold(dataframe, |df, expr| {
        df.filter(expr).context(error::PlanSqlSnafu)
    })?;

    // Apply `like` predicate if exists
    let dataframe = if let (ShowKind::Like(ident), Some(field)) = (&kind, like_field) {
        dataframe
            .filter(col(field).like(lit(ident.value.clone())))
            .context(error::PlanSqlSnafu)?
    } else {
        dataframe
    };

    // Apply sorting
    let dataframe = dataframe
        .sort(sort)
        .context(error::PlanSqlSnafu)?
        .select_columns(&projects.iter().map(|(c, _)| *c).collect::<Vec<_>>())
        .context(error::PlanSqlSnafu)?;

    // Apply projection
    let dataframe = projects
        .into_iter()
        .try_fold(dataframe, |df, (column, renamed_column)| {
            df.with_column_renamed(column, renamed_column)
                .context(error::PlanSqlSnafu)
        })?;

    let dataframe = match kind {
        ShowKind::All | ShowKind::Like(_) => {
            // Like kind is processed above
            dataframe
        }
        ShowKind::Where(filter) => {
            // Cast the results into VIEW for `where` clause,
            // which is evaluated against the column names displayed by the SHOW statement.
            let view = dataframe.into_view();
            let dataframe = SessionContext::new_with_state(
                query_engine
                    .engine_context(query_ctx.clone())
                    .state()
                    .clone(),
            )
            .read_table(view)
            .context(error::DataFusionSnafu)?;

            let planner = query_engine.planner();
            let planner = planner
                .as_any()
                .downcast_ref::<DfLogicalPlanner>()
                .expect("Must be the datafusion planner");

            let filter = planner
                .sql_to_expr(filter, dataframe.schema(), false, query_ctx)
                .await?;

            // Apply the `where` clause filters
            dataframe.filter(filter).context(error::PlanSqlSnafu)?
        }
    };

    let stream = dataframe
        .execute_stream()
        .await
        .context(error::DataFusionSnafu)?;

    Ok(Output::Stream(
        Box::pin(RecordBatchStreamAdapter::try_new(stream).context(error::CreateRecordBatchSnafu)?),
        None,
    ))
}

pub async fn show_tables(
    stmt: ShowTables,
    query_engine: &QueryEngineRef,
    catalog_manager: &CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let schema_name = if let Some(database) = stmt.database {
        database
    } else {
        query_ctx.current_schema().to_owned()
    };

    // (dennis): MySQL rename `table_name` to `Tables_in_{schema}`, but we use `Tables` instead.
    // I don't want to modify this currently, our dashboard may depend on it.
    let projects = if stmt.full {
        vec![
            (tables::TABLE_NAME, TABLES_COLUMN),
            (tables::TABLE_TYPE, TABLE_TYPE_COLUMN),
        ]
    } else {
        vec![(tables::TABLE_NAME, TABLES_COLUMN)]
    };
    let filters = vec![
        col(tables::TABLE_SCHEMA).eq(lit(schema_name.clone())),
        col(tables::TABLE_CATALOG).eq(lit(query_ctx.current_catalog())),
    ];
    let like_field = Some(tables::TABLE_NAME);
    let sort = vec![col(tables::TABLE_NAME).sort(true, true)];

    query_from_information_schema_table(
        query_engine,
        catalog_manager,
        query_ctx,
        TABLES,
        projects,
        filters,
        like_field,
        sort,
        stmt.kind,
    )
    .await
}

pub fn show_variable(stmt: ShowVariables, query_ctx: QueryContextRef) -> Result<Output> {
    let variable = stmt.variable.to_string().to_uppercase();
    let value = match variable.as_str() {
        "SYSTEM_TIME_ZONE" | "SYSTEM_TIMEZONE" => get_timezone(None).to_string(),
        "TIME_ZONE" | "TIMEZONE" => query_ctx.timezone().to_string(),
        _ => return UnsupportedVariableSnafu { name: variable }.fail(),
    };
    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        variable,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    let records = RecordBatches::try_from_columns(
        schema,
        vec![Arc::new(StringVector::from(vec![value])) as _],
    )
    .context(error::CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(records))
}

pub fn show_create_table(
    table: TableRef,
    partitions: Option<Partitions>,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let table_info = table.table_info();
    let table_name = &table_info.name;

    let quote_style = query_ctx.quote_style();

    let mut stmt = create_table_stmt(&table_info, quote_style)?;
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use common_query::Output;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use common_time::timestamp::TimeUnit;
    use common_time::Timezone;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{StringVector, TimestampMillisecondVector, UInt32Vector, VectorRef};
    use session::context::QueryContextBuilder;
    use snafu::ResultExt;
    use sql::ast::{Ident, ObjectName};
    use sql::statements::show::ShowVariables;
    use table::test_util::MemTable;
    use table::TableRef;

    use super::show_variable;
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

    #[test]
    fn test_show_variable() {
        assert_eq!(
            exec_show_variable("SYSTEM_TIME_ZONE", "Asia/Shanghai").unwrap(),
            "UTC"
        );
        assert_eq!(
            exec_show_variable("SYSTEM_TIMEZONE", "Asia/Shanghai").unwrap(),
            "UTC"
        );
        assert_eq!(
            exec_show_variable("TIME_ZONE", "Asia/Shanghai").unwrap(),
            "Asia/Shanghai"
        );
        assert_eq!(
            exec_show_variable("TIMEZONE", "Asia/Shanghai").unwrap(),
            "Asia/Shanghai"
        );
        assert!(exec_show_variable("TIME ZONE", "Asia/Shanghai").is_err());
        assert!(exec_show_variable("SYSTEM TIME ZONE", "Asia/Shanghai").is_err());
    }

    fn exec_show_variable(variable: &str, tz: &str) -> Result<String> {
        let stmt = ShowVariables {
            variable: ObjectName(vec![Ident::new(variable)]),
        };
        let ctx = QueryContextBuilder::default()
            .timezone(Arc::new(Timezone::from_tz_string(tz).unwrap()))
            .build();
        match show_variable(stmt, ctx) {
            Ok(Output::RecordBatches(record)) => {
                let record = record.take().first().cloned().unwrap();
                let data = record.column(0);
                Ok(data.get(0).to_string())
            }
            Ok(_) => unreachable!(),
            Err(e) => Err(e),
        }
    }
}
