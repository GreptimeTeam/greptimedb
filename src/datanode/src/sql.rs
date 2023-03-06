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

use catalog::CatalogManagerRef;
use common_procedure::ProcedureManagerRef;
use common_query::Output;
use common_telemetry::error;
use query::query_engine::QueryEngineRef;
use query::sql::{describe_table, explain, show_databases, show_tables};
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};
use sql::statements::delete::Delete;
use sql::statements::describe::DescribeTable;
use sql::statements::explain::Explain;
use sql::statements::show::{ShowDatabases, ShowTables};
use table::engine::{EngineContext, TableEngineProcedureRef, TableEngineRef, TableReference};
use table::requests::*;
use table::TableRef;

use crate::error::{self, ExecuteSqlSnafu, GetTableSnafu, Result, TableNotFoundSnafu};
use crate::instance::sql::table_idents_to_full_name;

mod alter;
mod copy_table;
mod create;
mod delete;
mod drop_table;
pub(crate) mod insert;

#[derive(Debug)]
pub enum SqlRequest {
    Insert(InsertRequest),
    CreateTable(CreateTableRequest),
    CreateDatabase(CreateDatabaseRequest),
    Alter(AlterTableRequest),
    DropTable(DropTableRequest),
    ShowDatabases(ShowDatabases),
    ShowTables(ShowTables),
    DescribeTable(DescribeTable),
    Explain(Box<Explain>),
    Delete(Delete),
    CopyTable(CopyTableRequest),
}

// Handler to execute SQL except query
pub struct SqlHandler {
    table_engine: TableEngineRef,
    catalog_manager: CatalogManagerRef,
    query_engine: QueryEngineRef,
    engine_procedure: TableEngineProcedureRef,
    procedure_manager: Option<ProcedureManagerRef>,
}

impl SqlHandler {
    pub fn new(
        table_engine: TableEngineRef,
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
        engine_procedure: TableEngineProcedureRef,
        procedure_manager: Option<ProcedureManagerRef>,
    ) -> Self {
        Self {
            table_engine,
            catalog_manager,
            query_engine,
            engine_procedure,
            procedure_manager,
        }
    }

    // TODO(LFC): Refactor consideration: a context awareness "Planner".
    // Now we have some query related state (like current using database in session context), maybe
    // we could create a new struct called `Planner` that stores context and handle these queries
    // there, instead of executing here in a "static" fashion.
    pub async fn execute(&self, request: SqlRequest, query_ctx: QueryContextRef) -> Result<Output> {
        let result = match request {
            SqlRequest::Insert(req) => self.insert(req).await,
            SqlRequest::CreateTable(req) => self.create_table(req).await,
            SqlRequest::CreateDatabase(req) => self.create_database(req, query_ctx.clone()).await,
            SqlRequest::Alter(req) => self.alter(req).await,
            SqlRequest::DropTable(req) => self.drop_table(req).await,
            SqlRequest::Delete(req) => self.delete(query_ctx.clone(), req).await,
            SqlRequest::CopyTable(req) => self.copy_table(req).await,
            SqlRequest::ShowDatabases(req) => {
                show_databases(req, self.catalog_manager.clone()).context(ExecuteSqlSnafu)
            }
            SqlRequest::ShowTables(req) => {
                show_tables(req, self.catalog_manager.clone(), query_ctx.clone())
                    .context(ExecuteSqlSnafu)
            }
            SqlRequest::DescribeTable(req) => {
                let (catalog, schema, table) =
                    table_idents_to_full_name(req.name(), query_ctx.clone())?;
                let table = self
                    .catalog_manager
                    .table(&catalog, &schema, &table)
                    .await
                    .context(error::CatalogSnafu)?
                    .with_context(|| TableNotFoundSnafu {
                        table_name: req.name().to_string(),
                    })?;
                describe_table(table).context(ExecuteSqlSnafu)
            }
            SqlRequest::Explain(req) => explain(req, self.query_engine.clone(), query_ctx.clone())
                .await
                .context(ExecuteSqlSnafu),
        };
        if let Err(e) = &result {
            error!(e; "{query_ctx}");
        }
        result
    }

    pub(crate) fn get_table(&self, table_ref: &TableReference) -> Result<TableRef> {
        self.table_engine
            .get_table(&EngineContext::default(), table_ref)
            .with_context(|_| GetTableSnafu {
                table_name: table_ref.to_string(),
            })?
            .with_context(|| TableNotFoundSnafu {
                table_name: table_ref.to_string(),
            })
    }

    pub fn table_engine(&self) -> TableEngineRef {
        self.table_engine.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use catalog::{CatalogManager, RegisterTableRequest};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_query::logical_plan::Expr;
    use common_query::physical_plan::PhysicalPlanRef;
    use common_test_util::temp_dir::create_temp_dir;
    use common_time::timestamp::Timestamp;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use datatypes::value::Value;
    use futures::StreamExt;
    use log_store::NoopLogStore;
    use mito::config::EngineConfig as TableEngineConfig;
    use mito::engine::MitoEngine;
    use object_store::services::Fs as Builder;
    use object_store::{ObjectStore, ObjectStoreBuilder};
    use query::parser::{QueryLanguageParser, QueryStatement};
    use query::QueryEngineFactory;
    use session::context::QueryContext;
    use sql::statements::statement::Statement;
    use storage::compaction::noop::NoopCompactionScheduler;
    use storage::config::EngineConfig as StorageEngineConfig;
    use storage::EngineImpl;
    use table::error::Result as TableResult;
    use table::metadata::TableInfoRef;
    use table::Table;

    use super::*;
    use crate::error::Error;
    use crate::sql::insert::InsertRequests;

    struct DemoTable;

    #[async_trait::async_trait]
    impl Table for DemoTable {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn schema(&self) -> SchemaRef {
            let column_schemas = vec![
                ColumnSchema::new("host", ConcreteDataType::string_datatype(), false),
                ColumnSchema::new("cpu", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new("memory", ConcreteDataType::float64_datatype(), true),
                ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    true,
                )
                .with_time_index(true),
            ];

            Arc::new(
                SchemaBuilder::try_from(column_schemas)
                    .unwrap()
                    .build()
                    .unwrap(),
            )
        }

        fn table_info(&self) -> TableInfoRef {
            unimplemented!()
        }

        async fn scan(
            &self,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<PhysicalPlanRef> {
            unimplemented!();
        }
    }

    #[tokio::test]
    async fn test_statement_to_request() {
        let dir = create_temp_dir("setup_test_engine_and_table");
        let store_dir = dir.path().to_string_lossy();
        let accessor = Builder::default().root(&store_dir).build().unwrap();
        let object_store = ObjectStore::new(accessor).finish();
        let compaction_scheduler = Arc::new(NoopCompactionScheduler::default());
        let sql = r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#;

        let table_engine = Arc::new(MitoEngine::<EngineImpl<NoopLogStore>>::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(NoopLogStore::default()),
                object_store.clone(),
                compaction_scheduler,
            ),
            object_store,
        ));

        let catalog_list = Arc::new(
            catalog::local::LocalCatalogManager::try_new(table_engine.clone())
                .await
                .unwrap(),
        );
        catalog_list.start().await.unwrap();
        assert!(catalog_list
            .register_table(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: "demo".to_string(),
                table_id: 1,
                table: Arc::new(DemoTable),
            })
            .await
            .unwrap());

        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine();
        let sql_handler = SqlHandler::new(
            table_engine.clone(),
            catalog_list.clone(),
            query_engine.clone(),
            table_engine,
            None,
        );

        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(Statement::Insert(i)) => i,
            _ => {
                unreachable!()
            }
        };
        let request = sql_handler
            .insert_to_requests(catalog_list.clone(), *stmt, QueryContext::arc())
            .await
            .unwrap();

        match request {
            InsertRequests::Request(SqlRequest::Insert(req)) => {
                assert_eq!(req.table_name, "demo");
                let columns_values = req.columns_values;
                assert_eq!(4, columns_values.len());

                let hosts = &columns_values["host"];
                assert_eq!(2, hosts.len());
                assert_eq!(Value::from("host1"), hosts.get(0));
                assert_eq!(Value::from("host2"), hosts.get(1));

                let cpus = &columns_values["cpu"];
                assert_eq!(2, cpus.len());
                assert_eq!(Value::from(66.6f64), cpus.get(0));
                assert_eq!(Value::from(88.8f64), cpus.get(1));

                let memories = &columns_values["memory"];
                assert_eq!(2, memories.len());
                assert_eq!(Value::from(1024f64), memories.get(0));
                assert_eq!(Value::from(333.3f64), memories.get(1));

                let ts = &columns_values["ts"];
                assert_eq!(2, ts.len());
                assert_eq!(
                    Value::from(Timestamp::new_millisecond(1655276557000i64)),
                    ts.get(0)
                );
                assert_eq!(
                    Value::from(Timestamp::new_millisecond(1655276558000i64)),
                    ts.get(1)
                );
            }
            _ => {
                panic!("Not supposed to reach here")
            }
        }

        // test inert into select

        // type mismatch
        let sql = "insert into demo(ts) select number from numbers limit 3";

        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(Statement::Insert(i)) => i,
            _ => {
                unreachable!()
            }
        };
        let request = sql_handler
            .insert_to_requests(catalog_list.clone(), *stmt, QueryContext::arc())
            .await
            .unwrap();

        match request {
            InsertRequests::Stream(mut stream) => {
                assert!(matches!(
                    stream.next().await.unwrap().unwrap_err(),
                    Error::ColumnTypeMismatch { .. }
                ));
            }
            _ => unreachable!(),
        }

        let sql = "insert into demo(cpu) select cast(number as double) from numbers limit 3";
        let stmt = match QueryLanguageParser::parse_sql(sql).unwrap() {
            QueryStatement::Sql(Statement::Insert(i)) => i,
            _ => {
                unreachable!()
            }
        };
        let request = sql_handler
            .insert_to_requests(catalog_list.clone(), *stmt, QueryContext::arc())
            .await
            .unwrap();

        match request {
            InsertRequests::Stream(mut stream) => {
                let mut times = 0;
                while let Some(Ok(SqlRequest::Insert(req))) = stream.next().await {
                    times += 1;
                    assert_eq!(req.table_name, "demo");
                    let columns_values = req.columns_values;
                    assert_eq!(1, columns_values.len());

                    let memories = &columns_values["cpu"];
                    assert_eq!(3, memories.len());
                    assert_eq!(Value::from(0.0f64), memories.get(0));
                    assert_eq!(Value::from(1.0f64), memories.get(1));
                    assert_eq!(Value::from(2.0f64), memories.get(2));
                }
                assert_eq!(1, times);
            }
            _ => unreachable!(),
        }
    }
}
