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

use std::collections::HashMap;

use catalog::{RegisterSchemaRequest, RegisterTableRequest};
use common_procedure::{watcher, ProcedureManagerRef, ProcedureWithId};
use common_query::Output;
use common_telemetry::tracing::{error, info};
use datatypes::schema::RawSchema;
use session::context::QueryContextRef;
use snafu::{ensure, OptionExt, ResultExt};
use sql::ast::{ColumnOption, SqlOption, TableConstraint, Value};
use sql::statements::column_def_to_schema;
use sql::statements::create::{CreateTable, TIME_INDEX};
use table::engine::{EngineContext, TableReference};
use table::metadata::TableId;
use table::requests::*;
use table_procedure::CreateTableProcedure;

use crate::error::{
    self, CatalogNotFoundSnafu, CatalogSnafu, ConstraintNotSupportedSnafu, CreateTableSnafu,
    IllegalPrimaryKeysDefSnafu, InsertSystemCatalogSnafu, KeyColumnNotFoundSnafu,
    RegisterSchemaSnafu, Result, SchemaExistsSnafu, SchemaNotFoundSnafu, SubmitProcedureSnafu,
    TableEngineNotFoundSnafu, UnrecognizedTableOptionSnafu, WaitProcedureSnafu,
};
use crate::sql::SqlHandler;

impl SqlHandler {
    pub(crate) async fn create_database(
        &self,
        req: CreateDatabaseRequest,
        query_ctx: QueryContextRef,
    ) -> Result<Output> {
        let catalog = query_ctx.current_catalog();
        let schema = req.db_name;
        if self
            .catalog_manager
            .schema(&catalog, &schema)
            .context(CatalogSnafu)?
            .is_some()
        {
            return if req.create_if_not_exists {
                Ok(Output::AffectedRows(1))
            } else {
                SchemaExistsSnafu { name: schema }.fail()
            };
        }

        let reg_req = RegisterSchemaRequest {
            catalog,
            schema: schema.clone(),
        };
        self.catalog_manager
            .register_schema(reg_req)
            .await
            .context(RegisterSchemaSnafu)?;

        info!("Successfully created database: {:?}", schema);
        Ok(Output::AffectedRows(1))
    }

    pub(crate) async fn create_table(&self, req: CreateTableRequest) -> Result<Output> {
        if let Some(procedure_manager) = &self.procedure_manager {
            return self.create_table_by_procedure(procedure_manager, req).await;
        }

        let ctx = EngineContext {};
        // first check if catalog and schema exist
        let catalog = self
            .catalog_manager
            .catalog(&req.catalog_name)
            .context(CatalogSnafu)?
            .with_context(|| {
                error!(
                    "Failed to create table {}.{}.{}, catalog not found",
                    &req.catalog_name, &req.schema_name, &req.table_name
                );
                CatalogNotFoundSnafu {
                    name: &req.catalog_name,
                }
            })?;
        catalog
            .schema(&req.schema_name)
            .context(CatalogSnafu)?
            .with_context(|| {
                error!(
                    "Failed to create table {}.{}.{}, schema not found",
                    &req.catalog_name, &req.schema_name, &req.table_name
                );
                SchemaNotFoundSnafu {
                    name: &req.schema_name,
                }
            })?;

        // determine catalog and schema from the very beginning
        let table_name = req.table_name.clone();
        let table_engine =
            self.table_engine_manager
                .engine(&req.engine)
                .context(TableEngineNotFoundSnafu {
                    engine_name: &req.engine,
                })?;

        let table = table_engine
            .create_table(&ctx, req)
            .await
            .with_context(|_| CreateTableSnafu {
                table_name: &table_name,
            })?;

        let register_req = RegisterTableRequest {
            catalog: table.table_info().catalog_name.clone(),
            schema: table.table_info().schema_name.clone(),
            table_name: table_name.clone(),
            table_id: table.table_info().ident.table_id,
            table,
        };

        self.catalog_manager
            .register_table(register_req)
            .await
            .context(InsertSystemCatalogSnafu)?;
        info!("Successfully created table: {:?}", table_name);
        // TODO(hl): maybe support create multiple tables
        Ok(Output::AffectedRows(0))
    }

    pub(crate) async fn create_table_by_procedure(
        &self,
        procedure_manager: &ProcedureManagerRef,
        req: CreateTableRequest,
    ) -> Result<Output> {
        let table_name = req.table_name.clone();
        let table_engine =
            self.table_engine_manager
                .engine(&req.engine)
                .context(TableEngineNotFoundSnafu {
                    engine_name: &req.engine,
                })?;
        let procedure = CreateTableProcedure::new(
            req,
            self.catalog_manager.clone(),
            table_engine.clone(),
            self.engine_procedure.clone(),
        );
        let procedure_with_id = ProcedureWithId::with_random_id(Box::new(procedure));
        let procedure_id = procedure_with_id.id;

        info!("Create table {} by procedure {}", table_name, procedure_id);

        let mut watcher = procedure_manager
            .submit(procedure_with_id)
            .await
            .context(SubmitProcedureSnafu { procedure_id })?;

        watcher::wait(&mut watcher)
            .await
            .context(WaitProcedureSnafu { procedure_id })?;

        Ok(Output::AffectedRows(0))
    }

    /// Converts [CreateTable] to [SqlRequest::CreateTable].
    pub(crate) fn create_to_request(
        &self,
        table_id: TableId,
        stmt: CreateTable,
        table_ref: &TableReference,
    ) -> Result<CreateTableRequest> {
        let mut ts_index = usize::MAX;
        let mut primary_keys = vec![];

        let col_map = stmt
            .columns
            .iter()
            .map(|e| e.name.value.clone())
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect::<HashMap<_, _>>();

        let pk_map = stmt
            .columns
            .iter()
            .filter(|col| {
                col.options.iter().any(|options| match options.option {
                    ColumnOption::Unique { is_primary } => is_primary,
                    _ => false,
                })
            })
            .map(|col| col.name.value.clone())
            .collect::<Vec<_>>();

        ensure!(
            pk_map.len() < 2,
            IllegalPrimaryKeysDefSnafu {
                msg: "not allowed to inline multiple primary keys in columns options"
            }
        );

        if let Some(pk) = pk_map.first() {
            // # Safety: Both pk_map and col_map are collected from stmt.columns
            primary_keys.push(*col_map.get(pk).unwrap());
        }

        for c in stmt.constraints {
            match c {
                TableConstraint::Unique {
                    name,
                    columns,
                    is_primary,
                } => {
                    if let Some(name) = name {
                        if name.value == TIME_INDEX {
                            ts_index = *col_map.get(&columns[0].value).context(
                                KeyColumnNotFoundSnafu {
                                    name: columns[0].value.to_string(),
                                },
                            )?;
                        } else {
                            return error::InvalidSqlSnafu {
                                msg: format!("Cannot recognize named UNIQUE constraint: {name}"),
                            }
                            .fail();
                        }
                    } else if is_primary {
                        if !primary_keys.is_empty() {
                            return IllegalPrimaryKeysDefSnafu {
                                msg: "found definitions of primary keys in multiple places",
                            }
                            .fail();
                        }
                        for col in columns {
                            primary_keys.push(*col_map.get(&col.value).context(
                                KeyColumnNotFoundSnafu {
                                    name: col.value.to_string(),
                                },
                            )?);
                        }
                    } else {
                        return error::InvalidSqlSnafu {
                            msg: format!(
                                "Unrecognized non-primary unnamed UNIQUE constraint: {name:?}",
                            ),
                        }
                        .fail();
                    }
                }
                _ => {
                    return ConstraintNotSupportedSnafu {
                        constraint: format!("{c:?}"),
                    }
                    .fail();
                }
            }
        }

        ensure!(
            !primary_keys.iter().any(|index| *index == ts_index),
            IllegalPrimaryKeysDefSnafu {
                msg: "time index column can't be included in primary key"
            }
        );

        ensure!(ts_index != usize::MAX, error::MissingTimestampColumnSnafu);

        let columns_schemas: Vec<_> = stmt
            .columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                column_def_to_schema(column, index == ts_index).context(error::ParseSqlSnafu)
            })
            .collect::<Result<Vec<_>>>()?;

        let table_options = stmt_options_to_table_options(&stmt.options)?;
        let schema = RawSchema::new(columns_schemas);
        let request = CreateTableRequest {
            id: table_id,
            catalog_name: table_ref.catalog.to_string(),
            schema_name: table_ref.schema.to_string(),
            table_name: table_ref.table.to_string(),
            desc: None,
            schema,
            region_numbers: vec![0],
            primary_key_indices: primary_keys,
            create_if_not_exists: stmt.if_not_exists,
            table_options,
            engine: stmt.engine,
        };
        Ok(request)
    }
}

fn stmt_options_to_table_options(opts: &[SqlOption]) -> error::Result<TableOptions> {
    let mut map = HashMap::with_capacity(opts.len());
    for SqlOption { name, value } in opts {
        let value_str = match value {
            Value::SingleQuotedString(s) => s.clone(),
            Value::DoubleQuotedString(s) => s.clone(),
            _ => value.to_string(),
        };
        map.insert(name.value.clone(), value_str);
    }
    let options = TableOptions::try_from(&map).context(UnrecognizedTableOptionSnafu)?;
    Ok(options)
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::time::Duration;

    use common_base::readable_size::ReadableSize;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::Schema;
    use query::parser::{QueryLanguageParser, QueryStatement};
    use query::query_engine::SqlStatementExecutor;
    use session::context::QueryContext;
    use sql::dialect::GenericDialect;
    use sql::parser::ParserContext;
    use sql::statements::statement::Statement;

    use super::*;
    use crate::error::Error;
    use crate::tests::test_util::{create_mock_sql_handler, MockInstance};

    fn sql_to_statement(sql: &str) -> CreateTable {
        let mut res = ParserContext::create_with_dialect(sql, &GenericDialect {}).unwrap();
        assert_eq!(1, res.len());
        match res.pop().unwrap() {
            Statement::CreateTable(c) => c,
            _ => {
                panic!("Unexpected statement!")
            }
        }
    }

    #[tokio::test]
    async fn test_create_table_with_options() {
        let sql = r#"
            CREATE TABLE demo_table (
                "timestamp" BIGINT TIME INDEX,
                "value" DOUBLE,
                host STRING PRIMARY KEY
            ) engine=mito with(regions=1, ttl='7days',write_buffer_size='32MB',some='other');"#;
        let parsed_stmt = sql_to_statement(sql);
        let handler = create_mock_sql_handler().await;
        let c = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap();

        assert_eq!(Some(Duration::from_secs(604800)), c.table_options.ttl);
        assert_eq!(
            Some(ReadableSize::mb(32)),
            c.table_options.write_buffer_size
        );
        assert_eq!("other", c.table_options.extra_options.get("some").unwrap());
    }

    #[tokio::test]
    pub async fn test_create_with_inline_primary_key() {
        let handler = create_mock_sql_handler().await;
        let parsed_stmt = sql_to_statement(
            r#"
            CREATE TABLE demo_table(
                "timestamp" BIGINT TIME INDEX,
                "value" DOUBLE,
                host STRING PRIMARY KEY
            ) engine=mito with(regions=1);"#,
        );
        let c = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap();
        assert_eq!("demo_table", c.table_name);
        assert_eq!(42, c.id);
        assert!(!c.create_if_not_exists);
        assert_eq!(vec![2], c.primary_key_indices);
    }

    #[tokio::test]
    pub async fn test_create_to_request() {
        let handler = create_mock_sql_handler().await;
        let parsed_stmt = sql_to_statement(
            r#"create table demo_table(
                       host string,
                       ts timestamp,
                       cpu double default 0,
                       memory double,
                       TIME INDEX (ts),
                       PRIMARY KEY(host)) engine=mito with(regions=1);"#,
        );
        let c = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap();
        assert_eq!("demo_table", c.table_name);
        assert_eq!(42, c.id);
        assert!(!c.create_if_not_exists);
        assert_eq!(vec![0], c.primary_key_indices);
        assert_eq!(1, c.schema.timestamp_index.unwrap());
        assert_eq!(4, c.schema.column_schemas.len());
    }

    #[tokio::test]
    pub async fn test_multiple_primary_key_definitions() {
        let handler = create_mock_sql_handler().await;
        let parsed_stmt = sql_to_statement(
            r#"create table demo_table (
                      "timestamp" BIGINT TIME INDEX,
                      "value" DOUBLE,
                      host STRING PRIMARY KEY,
                      PRIMARY KEY(host)) engine=mito with(regions=1);"#,
        );
        let error = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap_err();
        assert_matches!(error, Error::IllegalPrimaryKeysDef { .. });
    }

    #[tokio::test]
    pub async fn test_multiple_inline_primary_key_definitions() {
        let handler = create_mock_sql_handler().await;
        let parsed_stmt = sql_to_statement(
            r#"create table demo_table (
                      "timestamp" BIGINT TIME INDEX,
                      "value" DOUBLE PRIMARY KEY,
                      host STRING PRIMARY KEY) engine=mito with(regions=1);"#,
        );
        let error = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap_err();
        assert_matches!(error, Error::IllegalPrimaryKeysDef { .. });
    }

    #[tokio::test]
    pub async fn test_primary_key_not_specified() {
        let handler = create_mock_sql_handler().await;

        let parsed_stmt = sql_to_statement(
            r#"create table demo_table(
                      host string,
                      ts timestamp,
                      cpu double default 0,
                      memory double,
                      TIME INDEX (ts)) engine=mito with(regions=1);"#,
        );
        let c = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap();
        assert!(c.primary_key_indices.is_empty());
        assert_eq!(c.schema.timestamp_index, Some(1));
    }

    /// Constraints specified, not column cannot be found.
    #[tokio::test]
    pub async fn test_key_not_found() {
        let handler = create_mock_sql_handler().await;

        let parsed_stmt = sql_to_statement(
            r#"create table demo_table(
                host string,
                TIME INDEX (ts)) engine=mito with(regions=1);"#,
        );

        let error = handler
            .create_to_request(42, parsed_stmt, &TableReference::bare("demo_table"))
            .unwrap_err();
        assert_matches!(error, Error::KeyColumnNotFound { .. });
    }

    #[tokio::test]
    pub async fn test_invalid_primary_key() {
        let create_table = sql_to_statement(
            r"create table c.s.demo(
                             host string,
                             ts timestamp,
                             cpu double default 0,
                             memory double,
                             TIME INDEX (ts),
                             PRIMARY KEY(host, cpu, ts)) engine=mito
                             with(regions=1);
         ",
        );

        let handler = create_mock_sql_handler().await;

        let error = handler
            .create_to_request(42, create_table, &TableReference::full("c", "s", "demo"))
            .unwrap_err();
        assert_matches!(error, Error::IllegalPrimaryKeysDef { .. });
    }

    #[tokio::test]
    pub async fn test_parse_create_sql() {
        let create_table = sql_to_statement(
            r"create table c.s.demo(
                             host string,
                             ts timestamp,
                             cpu double default 0,
                             memory double,
                             TIME INDEX (ts),
                             PRIMARY KEY(host)) engine=mito
                             with(regions=1);
         ",
        );

        let handler = create_mock_sql_handler().await;

        let request = handler
            .create_to_request(42, create_table, &TableReference::full("c", "s", "demo"))
            .unwrap();

        assert_eq!(42, request.id);
        assert_eq!("c".to_string(), request.catalog_name);
        assert_eq!("s".to_string(), request.schema_name);
        assert_eq!("demo".to_string(), request.table_name);
        assert!(!request.create_if_not_exists);
        assert_eq!(4, request.schema.column_schemas.len());

        assert_eq!(vec![0], request.primary_key_indices);
        let schema = Schema::try_from(request.schema).unwrap();
        assert_eq!(
            ConcreteDataType::string_datatype(),
            schema.column_schema_by_name("host").unwrap().data_type
        );
        assert_eq!(
            ConcreteDataType::timestamp_millisecond_datatype(),
            schema.column_schema_by_name("ts").unwrap().data_type
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            schema.column_schema_by_name("cpu").unwrap().data_type
        );
        assert_eq!(
            ConcreteDataType::float64_datatype(),
            schema.column_schema_by_name("memory").unwrap().data_type
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn create_table_by_procedure() {
        let instance = MockInstance::with_procedure_enabled("create_table_by_procedure").await;

        let sql = r#"create table test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let Ok(QueryStatement::Sql(stmt)) = QueryLanguageParser::parse_sql(sql) else { unreachable!() };
        let output = instance
            .inner()
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));

        // create if not exists
        let sql = r#"create table if not exists test_table(
                            host string,
                            ts timestamp,
                            cpu double default 0,
                            memory double,
                            TIME INDEX (ts),
                            PRIMARY KEY(host)
                        ) engine=mito with(regions=1);"#;
        let Ok(QueryStatement::Sql(stmt)) = QueryLanguageParser::parse_sql(sql) else { unreachable!() };
        let output = instance
            .inner()
            .execute_sql(stmt, QueryContext::arc())
            .await
            .unwrap();
        assert!(matches!(output, Output::AffectedRows(0)));
    }
}
