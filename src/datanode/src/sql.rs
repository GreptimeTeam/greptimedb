//! sql handler

use std::sync::Arc;

use catalog::CatalogManagerRef;
use common_error::ext::BoxedError;
use query::query_engine::Output;
use snafu::{OptionExt, ResultExt};
use table::engine::{EngineContext, TableEngine};
use table::requests::*;
use table::TableRef;

use crate::error::{GetTableSnafu, Result, TableNotFoundSnafu};

mod create;
mod insert;

#[derive(Debug)]
pub enum SqlRequest {
    Insert(InsertRequest),
    Create(CreateTableRequest),
}

// Handler to execute SQL except query
pub struct SqlHandler<Engine: TableEngine> {
    table_engine: Arc<Engine>,
    catalog_manager: CatalogManagerRef,
}

impl<Engine: TableEngine> SqlHandler<Engine> {
    pub fn new(table_engine: Engine, catalog_manager: CatalogManagerRef) -> Self {
        Self {
            table_engine: Arc::new(table_engine),
            catalog_manager,
        }
    }

    pub async fn execute(&self, request: SqlRequest) -> Result<Output> {
        match request {
            SqlRequest::Insert(req) => self.insert(req).await,
            SqlRequest::Create(req) => self.create(req).await,
        }
    }

    pub(crate) fn get_table(&self, table_name: &str) -> Result<TableRef> {
        self.table_engine
            .get_table(&EngineContext::default(), table_name)
            .map_err(BoxedError::new)
            .context(GetTableSnafu { table_name })?
            .context(TableNotFoundSnafu { table_name })
    }

    pub fn table_engine(&self) -> Arc<Engine> {
        self.table_engine.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::any::Any;
    use std::sync::Arc;

    use catalog::SchemaProvider;
    use common_query::logical_plan::Expr;
    use common_recordbatch::SendableRecordBatchStream;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::value::Value;
    use log_store::fs::noop::NoopLogStore;
    use object_store::{backend::fs::Backend, ObjectStore};
    use query::QueryEngineFactory;
    use sql::statements::statement::Statement;
    use storage::config::EngineConfig as StorageEngineConfig;
    use storage::EngineImpl;
    use table::error::Result as TableResult;
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
                ColumnSchema::new("ts", ConcreteDataType::int64_datatype(), true),
            ];

            Arc::new(Schema::with_timestamp_index(column_schemas, 3).unwrap())
        }
        async fn scan(
            &self,
            _projection: &Option<Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> TableResult<SendableRecordBatchStream> {
            unimplemented!();
        }
    }

    struct MockSchemaProvider;

    impl SchemaProvider for MockSchemaProvider {
        fn as_any(&self) -> &dyn Any {
            self
        }

        fn table_names(&self) -> Vec<String> {
            vec!["demo".to_string()]
        }

        fn table(&self, name: &str) -> Option<TableRef> {
            assert_eq!(name, "demo");
            Some(Arc::new(DemoTable {}))
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
        fn table_exist(&self, name: &str) -> bool {
            name == "demo"
        }
    }

    #[tokio::test]
    async fn test_statement_to_request() {
        let dir = TempDir::new("setup_test_engine_and_table").unwrap();
        let store_dir = dir.path().to_string_lossy();
        let accessor = Backend::build().root(&store_dir).finish().await.unwrap();
        let object_store = ObjectStore::new(accessor);

        let sql = r#"insert into demo(host, cpu, memory, ts) values
                           ('host1', 66.6, 1024, 1655276557000),
                           ('host2', 88.8,  333.3, 1655276558000)
                           "#;

        let table_engine = MitoEngine::<EngineImpl<NoopLogStore>>::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(NoopLogStore::default()),
                object_store.clone(),
            ),
            object_store,
        );

        let catalog_list = Arc::new(
            catalog::LocalCatalogManager::try_new(Arc::new(table_engine.clone()))
                .await
                .unwrap(),
        );
        let factory = QueryEngineFactory::new(catalog_list.clone());
        let query_engine = factory.query_engine().clone();
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
                assert_eq!(Value::from(1655276557000i64), ts.get(0));
                assert_eq!(Value::from(1655276558000i64), ts.get(1));
            }
            _ => {
                panic!("Not supposed to reach here")
            }
        }
    }
}
