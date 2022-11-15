// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! sql handler

use catalog::CatalogManagerRef;
use common_query::Output;
use query::sql::{show_databases, show_tables};
use snafu::{OptionExt, ResultExt};
use sql::statements::show::{ShowDatabases, ShowTables};
use table::engine::{EngineContext, TableEngineRef, TableReference};
use table::requests::*;
use table::TableRef;

use crate::error::{self, GetTableSnafu, Result, TableNotFoundSnafu};

mod alter;
mod create;
mod insert;

#[derive(Debug)]
pub enum SqlRequest {
    Insert(InsertRequest),
    CreateTable(CreateTableRequest),
    CreateDatabase(CreateDatabaseRequest),
    Alter(AlterTableRequest),
    ShowDatabases(ShowDatabases),
    ShowTables(ShowTables),
}

// Handler to execute SQL except query
pub struct SqlHandler {
    table_engine: TableEngineRef,
    catalog_manager: CatalogManagerRef,
}

impl SqlHandler {
    pub fn new(table_engine: TableEngineRef, catalog_manager: CatalogManagerRef) -> Self {
        Self {
            table_engine,
            catalog_manager,
        }
    }

    pub async fn execute(&self, request: SqlRequest) -> Result<Output> {
        match request {
            SqlRequest::Insert(req) => self.insert(req).await,
            SqlRequest::CreateTable(req) => self.create_table(req).await,
            SqlRequest::CreateDatabase(req) => self.create_database(req).await,
            SqlRequest::Alter(req) => self.alter(req).await,
            SqlRequest::ShowDatabases(stmt) => {
                show_databases(stmt, self.catalog_manager.clone()).context(error::ExecuteSqlSnafu)
            }
            SqlRequest::ShowTables(stmt) => {
                show_tables(stmt, self.catalog_manager.clone()).context(error::ExecuteSqlSnafu)
            }
        }
    }

    pub(crate) fn get_table<'a>(&self, table_ref: &'a TableReference) -> Result<TableRef> {
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

    use catalog::SchemaProvider;
    use common_query::logical_plan::Expr;
    use common_query::physical_plan::PhysicalPlanRef;
    use common_time::timestamp::Timestamp;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, SchemaBuilder, SchemaRef};
    use datatypes::value::Value;
    use log_store::fs::noop::NoopLogStore;
    use object_store::services::fs::Builder;
    use object_store::ObjectStore;
    use query::QueryEngineFactory;
    use sql::statements::statement::Statement;
    use storage::config::EngineConfig as StorageEngineConfig;
    use storage::EngineImpl;
    use table::error::Result as TableResult;
    use table::metadata::TableInfoRef;
    use table::{Table, TableRef};
    use table_engine::config::EngineConfig as TableEngineConfig;
    use table_engine::engine::MitoEngine;
    use tempdir::TempDir;

    use super::*;

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
                ColumnSchema::new("ts", ConcreteDataType::timestamp_millis_datatype(), true)
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
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<PhysicalPlanRef> {
            unimplemented!();
        }
    }

    struct MockSchemaProvider;

    impl SchemaProvider for MockSchemaProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_names(&self) -> catalog::error::Result<Vec<String>> {
            Ok(vec!["demo".to_string()])
        }

        fn table(&self, name: &str) -> catalog::error::Result<Option<TableRef>> {
            assert_eq!(name, "demo");
            Ok(Some(Arc::new(DemoTable {})))
        }

        fn register_table(
            &self,
            _name: String,
            _table: TableRef,
        ) -> catalog::error::Result<Option<TableRef>> {
            unimplemented!();
        }
        fn deregister_table(&self, _name: &str) -> catalog::error::Result<Option<TableRef>> {
            unimplemented!();
        }
        fn table_exist(&self, name: &str) -> catalog::error::Result<bool> {
            Ok(name == "demo")
        }
    }

    #[tokio::test]
    async fn test_statement_to_request() {
        let dir = TempDir::new("setup_test_engine_and_table").unwrap();
        let store_dir = dir.path().to_string_lossy();
        let accessor = Builder::default().root(&store_dir).build().unwrap();
        let object_store = ObjectStore::new(accessor);

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
            ),
            object_store,
        ));

        let catalog_list = Arc::new(
            catalog::local::LocalCatalogManager::try_new(table_engine.clone())
                .await
                .unwrap(),
        );
        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine();
        let sql_handler = SqlHandler::new(table_engine, catalog_list);

        let stmt = match query_engine.sql_to_statement(sql).unwrap() {
            Statement::Insert(i) => i,
            _ => {
                unreachable!()
            }
        };
        let schema_provider = Arc::new(MockSchemaProvider {});
        let request = sql_handler
            .insert_to_request(schema_provider, *stmt)
            .unwrap();

        match request {
            SqlRequest::Insert(req) => {
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
                    Value::from(Timestamp::from_millis(1655276557000i64)),
                    ts.get(0)
                );
                assert_eq!(
                    Value::from(Timestamp::from_millis(1655276558000i64)),
                    ts.get(1)
                );
            }
            _ => {
                panic!("Not supposed to reach here")
            }
        }
    }
}
