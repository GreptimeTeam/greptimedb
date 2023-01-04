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

use catalog::CatalogManagerRef;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::Output;
use common_recordbatch::RecordBatches;
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Helper, StringVector};
use once_cell::sync::Lazy;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::statements::describe::DescribeTable;
use sql::statements::explain::Explain;
use sql::statements::show::{ShowDatabases, ShowKind, ShowTables};
use sql::statements::statement::Statement;

use crate::error::{self, Result};
use crate::parser::QueryStatement;
use crate::QueryEngineRef;

const SCHEMAS_COLUMN: &str = "Schemas";
const TABLES_COLUMN: &str = "Tables";
const COLUMN_NAME_COLUMN: &str = "Field";
const COLUMN_TYPE_COLUMN: &str = "Type";
const COLUMN_NULLABLE_COLUMN: &str = "Null";
const COLUMN_DEFAULT_COLUMN: &str = "Default";
const COLUMN_SEMANTIC_TYPE_COLUMN: &str = "Semantic Type";

const SEMANTIC_TYPE_PRIMARY_KEY: &str = "PRIMARY KEY";
const SEMANTIC_TYPE_VALUE: &str = "VALUE";
const SEMANTIC_TYPE_TIME_INDEX: &str = "TIME INDEX";

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

pub fn show_databases(stmt: ShowDatabases, catalog_manager: CatalogManagerRef) -> Result<Output> {
    // TODO(LFC): supports WHERE
    ensure!(
        matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
        error::UnsupportedExprSnafu {
            name: stmt.kind.to_string(),
        }
    );

    let catalog = catalog_manager
        .catalog(DEFAULT_CATALOG_NAME)
        .context(error::CatalogSnafu)?
        .context(error::CatalogNotFoundSnafu {
            catalog: DEFAULT_CATALOG_NAME,
        })?;
    let mut databases = catalog.schema_names().context(error::CatalogSnafu)?;
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

pub fn show_tables(
    stmt: ShowTables,
    catalog_manager: CatalogManagerRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    // TODO(LFC): supports WHERE
    ensure!(
        matches!(stmt.kind, ShowKind::All | ShowKind::Like(_)),
        error::UnsupportedExprSnafu {
            name: stmt.kind.to_string(),
        }
    );

    let schema = if let Some(database) = stmt.database {
        database
    } else {
        query_ctx
            .current_schema()
            .unwrap_or_else(|| DEFAULT_SCHEMA_NAME.to_string())
    };
    // TODO(sunng87): move this function into query_ctx
    let catalog = query_ctx.current_catalog();
    let catalog = catalog.as_deref().unwrap_or(DEFAULT_CATALOG_NAME);
    let schema = catalog_manager
        .schema(catalog, &schema)
        .context(error::CatalogSnafu)?
        .context(error::SchemaNotFoundSnafu { schema })?;
    let mut tables = schema.table_names().context(error::CatalogSnafu)?;
    // TODO(dennis): Specify the order of the results in schema provider API
    tables.sort();

    let tables = if let ShowKind::Like(ident) = stmt.kind {
        Helper::like_utf8(tables, &ident.value).context(error::VectorComputationSnafu)?
    } else {
        Arc::new(StringVector::from(tables))
    };

    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        TABLES_COLUMN,
        ConcreteDataType::string_datatype(),
        false,
    )]));
    let records = RecordBatches::try_from_columns(schema, vec![tables])
        .context(error::CreateRecordBatchSnafu)?;
    Ok(Output::RecordBatches(records))
}

pub async fn explain(
    stmt: Box<Explain>,
    query_engine: QueryEngineRef,
    query_ctx: QueryContextRef,
) -> Result<Output> {
    let plan = query_engine
        .statement_to_plan(QueryStatement::Sql(Statement::Explain(*stmt)), query_ctx)?;
    query_engine.execute(&plan).await
}

pub fn describe_table(stmt: DescribeTable, catalog_manager: CatalogManagerRef) -> Result<Output> {
    let catalog = stmt.catalog_name.as_str();
    let schema = stmt.schema_name.as_str();
    catalog_manager
        .catalog(catalog)
        .context(error::CatalogSnafu)?
        .context(error::CatalogNotFoundSnafu { catalog })?;
    let schema = catalog_manager
        .schema(catalog, schema)
        .context(error::CatalogSnafu)?
        .context(error::SchemaNotFoundSnafu { schema })?;
    let table = schema
        .table(&stmt.table_name)
        .context(error::CatalogSnafu)?
        .context(error::TableNotFoundSnafu {
            table: &stmt.table_name,
        })?;

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
                    String::from(SEMANTIC_TYPE_VALUE)
                }
            })
            .collect::<Vec<String>>(),
    ))
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use catalog::local::{MemoryCatalogManager, MemoryCatalogProvider, MemorySchemaProvider};
    use catalog::{CatalogList, CatalogManagerRef, CatalogProvider, SchemaProvider};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::ErrorExt;
    use common_query::Output;
    use common_recordbatch::{RecordBatch, RecordBatches};
    use common_time::timestamp::TimeUnit;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnDefaultConstraint, ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{StringVector, TimestampMillisecondVector, UInt32Vector, VectorRef};
    use snafu::ResultExt;
    use sql::statements::describe::DescribeTable;
    use table::test_util::MemTable;

    use crate::error;
    use crate::error::Result;
    use crate::sql::{
        describe_table, DESCRIBE_TABLE_OUTPUT_SCHEMA, NULLABLE_NO, NULLABLE_YES,
        SEMANTIC_TYPE_TIME_INDEX, SEMANTIC_TYPE_VALUE,
    };

    #[test]
    fn test_describe_table_catalog_not_found() -> Result<()> {
        let catalog_name = DEFAULT_CATALOG_NAME.to_string();
        let schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let table_name = "test_table";
        let table_schema = SchemaRef::new(Schema::new(vec![ColumnSchema::new(
            "test_col",
            ConcreteDataType::uint32_datatype(),
            false,
        )]));
        let data = vec![Arc::new(UInt32Vector::from_vec(vec![0])) as _];
        let catalog_manager =
            prepare_describe_table(&catalog_name, &schema_name, table_name, table_schema, data);

        let stmt = DescribeTable::new("unknown".to_string(), schema_name, table_name.to_string());

        let err = describe_table(stmt, catalog_manager).err().unwrap();
        let err = err.as_any().downcast_ref::<error::Error>().unwrap();

        if let error::Error::CatalogNotFound { catalog, .. } = err {
            assert_eq!(catalog, "unknown");
        } else {
            panic!("describe table returned incorrect error");
        }

        Ok(())
    }

    #[test]
    fn test_describe_table_schema_not_found() -> Result<()> {
        let catalog_name = DEFAULT_CATALOG_NAME.to_string();
        let schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let table_name = "test_table";
        let table_schema = SchemaRef::new(Schema::new(vec![ColumnSchema::new(
            "test_col",
            ConcreteDataType::uint32_datatype(),
            false,
        )]));
        let data = vec![Arc::new(UInt32Vector::from_vec(vec![0])) as _];
        let catalog_manager =
            prepare_describe_table(&catalog_name, &schema_name, table_name, table_schema, data);

        let stmt = DescribeTable::new(catalog_name, "unknown".to_string(), table_name.to_string());

        let err = describe_table(stmt, catalog_manager).err().unwrap();
        let err = err.as_any().downcast_ref::<error::Error>().unwrap();

        if let error::Error::SchemaNotFound { schema, .. } = err {
            assert_eq!(schema, "unknown");
        } else {
            panic!("describe table returned incorrect error");
        }

        Ok(())
    }

    #[test]
    fn test_describe_table_table_not_found() -> Result<()> {
        let catalog_name = DEFAULT_CATALOG_NAME.to_string();
        let schema_name = DEFAULT_SCHEMA_NAME.to_string();
        let table_name = "test_table";
        let table_schema = SchemaRef::new(Schema::new(vec![ColumnSchema::new(
            "test_col",
            ConcreteDataType::uint32_datatype(),
            false,
        )]));
        let data = vec![Arc::new(UInt32Vector::from_vec(vec![0])) as _];
        let catalog_manager =
            prepare_describe_table(&catalog_name, &schema_name, table_name, table_schema, data);

        let stmt = DescribeTable::new(catalog_name, schema_name, "unknown".to_string());

        let err = describe_table(stmt, catalog_manager).err().unwrap();
        let err = err.as_any().downcast_ref::<error::Error>().unwrap();

        if let error::Error::TableNotFound { table, .. } = err {
            assert_eq!(table, "unknown");
        } else {
            panic!("describe table returned incorrect error");
        }

        Ok(())
    }

    #[test]
    fn test_describe_table_multiple_columns() -> Result<()> {
        let catalog_name = DEFAULT_CATALOG_NAME;
        let schema_name = DEFAULT_SCHEMA_NAME;
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
            Arc::new(UInt32Vector::from_slice(&[0])) as _,
            Arc::new(TimestampMillisecondVector::from_slice(&[0])) as _,
        ];
        let expected_columns = vec![
            Arc::new(StringVector::from(vec!["t1", "t2"])) as _,
            Arc::new(StringVector::from(vec!["UInt32", "TimestampMillisecond"])) as _,
            Arc::new(StringVector::from(vec![NULLABLE_YES, NULLABLE_NO])) as _,
            Arc::new(StringVector::from(vec!["", "current_timestamp()"])) as _,
            Arc::new(StringVector::from(vec![
                SEMANTIC_TYPE_VALUE,
                SEMANTIC_TYPE_TIME_INDEX,
            ])) as _,
        ];

        describe_table_test_by_schema(
            catalog_name,
            schema_name,
            table_name,
            schema,
            data,
            expected_columns,
        )
    }

    fn describe_table_test_by_schema(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        schema: Vec<ColumnSchema>,
        data: Vec<VectorRef>,
        expected_columns: Vec<VectorRef>,
    ) -> Result<()> {
        let table_schema = SchemaRef::new(Schema::new(schema));
        let catalog_manager =
            prepare_describe_table(catalog_name, schema_name, table_name, table_schema, data);

        let expected =
            RecordBatches::try_from_columns(DESCRIBE_TABLE_OUTPUT_SCHEMA.clone(), expected_columns)
                .context(error::CreateRecordBatchSnafu)?;

        let stmt = DescribeTable::new(
            catalog_name.to_string(),
            schema_name.to_string(),
            table_name.to_string(),
        );
        if let Output::RecordBatches(res) = describe_table(stmt, catalog_manager)? {
            assert_eq!(res.take(), expected.take());
        } else {
            panic!("describe table must return record batch");
        }

        Ok(())
    }

    fn prepare_describe_table(
        catalog_name: &str,
        schema_name: &str,
        table_name: &str,
        table_schema: SchemaRef,
        data: Vec<VectorRef>,
    ) -> CatalogManagerRef {
        let record_batch = RecordBatch::new(table_schema, data).unwrap();
        let table = Arc::new(MemTable::new(table_name, record_batch));

        let schema_provider = Arc::new(MemorySchemaProvider::new());
        let catalog_provider = Arc::new(MemoryCatalogProvider::new());
        let catalog_manager = Arc::new(MemoryCatalogManager::default());
        schema_provider
            .register_table(table_name.to_string(), table)
            .unwrap();
        catalog_provider
            .register_schema(schema_name.to_string(), schema_provider)
            .unwrap();
        catalog_manager
            .register_catalog(catalog_name.to_string(), catalog_provider)
            .unwrap();

        catalog_manager
    }
}
