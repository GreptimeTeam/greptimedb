use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use catalog::memory::{MemoryCatalogList, MemoryCatalogProvider, MemorySchemaProvider};
use catalog::{
    CatalogList, CatalogProvider, SchemaProvider, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
};
use common_query::Output;
use query::{QueryEngineFactory, QueryEngineRef};
use servers::error::Result;
use servers::query_handler::{SqlQueryHandler, SqlQueryHandlerRef};
use test_util::MemTable;

mod http;
mod mysql;
use script::{
    engine::{CompileContext, EvalContext, Script, ScriptEngine},
    python::{PyEngine, PyScript},
};
mod opentsdb;
mod postgres;

struct DummyInstance {
    query_engine: QueryEngineRef,
    py_engine: Arc<PyEngine>,
    scripts: RwLock<HashMap<String, Arc<PyScript>>>,
}

impl DummyInstance {
    fn new(query_engine: QueryEngineRef) -> Self {
        Self {
            py_engine: Arc::new(PyEngine::new(query_engine.clone())),
            scripts: RwLock::new(HashMap::new()),
            query_engine,
        }
    }
}

#[async_trait]
impl SqlQueryHandler for DummyInstance {
    async fn do_query(&self, query: &str) -> Result<Output> {
        let plan = self.query_engine.sql_to_plan(query).unwrap();
        Ok(self.query_engine.execute(&plan).await.unwrap())
    }

    async fn insert_script(&self, name: &str, script: &str) -> Result<()> {
        let script = self
            .py_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        self.scripts
            .write()
            .unwrap()
            .insert(name.to_string(), Arc::new(script));

        Ok(())
    }

    async fn execute_script(&self, name: &str) -> Result<Output> {
        let py_script = self.scripts.read().unwrap().get(name).unwrap().clone();

        Ok(py_script.execute(EvalContext::default()).await.unwrap())
    }
}

fn create_testing_sql_query_handler(table: MemTable) -> SqlQueryHandlerRef {
    let table_name = table.table_name().to_string();
    let table = Arc::new(table);

    let schema_provider = Arc::new(MemorySchemaProvider::new());
    let catalog_provider = Arc::new(MemoryCatalogProvider::new());
    let catalog_list = Arc::new(MemoryCatalogList::default());
    schema_provider.register_table(table_name, table).unwrap();
    catalog_provider.register_schema(DEFAULT_SCHEMA_NAME.to_string(), schema_provider);
    catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), catalog_provider);

    let factory = QueryEngineFactory::new(catalog_list);
    let query_engine = factory.query_engine().clone();
    Arc::new(DummyInstance::new(query_engine))
}
