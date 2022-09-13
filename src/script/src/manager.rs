//! Scripts manager
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use catalog::CatalogManagerRef;
use common_telemetry::logging;
use query::{Output, QueryEngineRef};
use snafu::{OptionExt, ResultExt};

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::error::{CompilePythonSnafu, ExecutePythonSnafu, Result, ScriptNotFoundSnafu};
use crate::python::{PyEngine, PyScript};
use crate::table::ScriptsTable;

pub struct ScriptManager {
    compiled: RwLock<HashMap<String, Arc<PyScript>>>,
    py_engine: PyEngine,
    table: ScriptsTable,
}

impl ScriptManager {
    pub async fn new(
        catalog_manager: CatalogManagerRef,
        query_engine: QueryEngineRef,
    ) -> Result<Self> {
        Ok(Self {
            compiled: RwLock::new(HashMap::default()),
            py_engine: PyEngine::new(query_engine.clone()),
            table: ScriptsTable::new(catalog_manager, query_engine).await?,
        })
    }

    async fn compile(&self, name: &str, script: &str) -> Result<Arc<PyScript>> {
        let script = Arc::new(
            self.py_engine
                .compile(script, CompileContext::default())
                .await
                .context(CompilePythonSnafu { name })?,
        );

        let mut compiled = self.compiled.write().unwrap();
        compiled.insert(name.to_string(), script.clone());

        logging::info!("Compiled and cached script: {}", name);

        Ok(script)
    }

    pub async fn insert_and_compile(&self, name: &str, script: &str) -> Result<Arc<PyScript>> {
        let compiled_script = self.compile(name, script).await?;
        self.table.insert(name, script).await?;
        Ok(compiled_script)
    }

    pub async fn execute(&self, name: &str) -> Result<Output> {
        let script = {
            let s = self.compiled.read().unwrap().get(name).cloned();

            if s.is_some() {
                s
            } else {
                self.try_find_script_and_compile(name).await?
            }
        };

        let script = script.context(ScriptNotFoundSnafu { name })?;

        script
            .execute(EvalContext::default())
            .await
            .context(ExecutePythonSnafu { name })
    }

    async fn try_find_script_and_compile(&self, name: &str) -> Result<Option<Arc<PyScript>>> {
        let script = self.table.find_script_by_name(name).await?;

        Ok(Some(self.compile(name, &script).await?))
    }
}

#[cfg(test)]
mod tests {
    use catalog::CatalogManager;
    use query::QueryEngineFactory;
    use table_engine::config::EngineConfig as TableEngineConfig;
    use table_engine::table::test_util::new_test_object_store;

    use super::*;
    type DefaultEngine = MitoEngine<EngineImpl<LocalFileLogStore>>;
    use log_store::fs::{config::LogConfig, log::LocalFileLogStore};
    use storage::{config::EngineConfig as StorageEngineConfig, EngineImpl};
    use table_engine::engine::MitoEngine;
    use tempdir::TempDir;

    #[tokio::test]
    async fn test_insert_find_compile_script() {
        let wal_dir = TempDir::new("test_insert_find_compile_script_wal").unwrap();
        let wal_dir_str = wal_dir.path().to_string_lossy();

        common_telemetry::init_default_ut_logging();
        let (_dir, object_store) = new_test_object_store("test_insert_find_compile_script").await;
        let log_config = LogConfig {
            log_file_dir: wal_dir_str.to_string(),
            ..Default::default()
        };

        let log_store = LocalFileLogStore::open(&log_config).await.unwrap();

        let mock_engine = Arc::new(DefaultEngine::new(
            TableEngineConfig::default(),
            EngineImpl::new(
                StorageEngineConfig::default(),
                Arc::new(log_store),
                object_store.clone(),
            ),
            object_store,
        ));

        let catalog_manager = Arc::new(
            catalog::LocalCatalogManager::try_new(mock_engine.clone())
                .await
                .unwrap(),
        );

        let factory = QueryEngineFactory::new(catalog_manager.clone());
        let query_engine = factory.query_engine().clone();
        let mgr = ScriptManager::new(catalog_manager.clone(), query_engine)
            .await
            .unwrap();
        catalog_manager.start().await.unwrap();

        let name = "test";
        mgr.table
            .insert(
                name,
                r#"
@copr(sql='select number from numbers limit 10', args=['number'], returns=['n'])
def test(n):
    return n + 1;
"#,
            )
            .await
            .unwrap();

        {
            let cached = mgr.compiled.read().unwrap();
            assert!(cached.get(name).is_none());
        }

        // try to find and compile
        let script = mgr.try_find_script_and_compile(name).await.unwrap();
        assert!(script.is_some());

        {
            let cached = mgr.compiled.read().unwrap();
            assert!(cached.get(name).is_some());
        }
    }
}
