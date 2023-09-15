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

//! Scripts manager
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use catalog::{CatalogManagerRef, OpenSystemTableHook, RegisterSystemTableRequest};
use common_catalog::consts::{
    default_engine, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME, SCRIPTS_TABLE_ID,
};
use common_query::Output;
use common_telemetry::logging;
use futures::future::FutureExt;
use query::QueryEngineRef;
use snafu::{OptionExt, ResultExt};
use table::requests::{CreateTableRequest, TableOptions};
use table::TableRef;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::error::{CompilePythonSnafu, ExecutePythonSnafu, Result, ScriptNotFoundSnafu};
use crate::python::{PyEngine, PyScript};
use crate::table::{build_scripts_schema, ScriptsTable, SCRIPTS_TABLE_NAME};

pub struct ScriptManager {
    compiled: RwLock<HashMap<String, Arc<PyScript>>>,
    py_engine: PyEngine,
    query_engine: QueryEngineRef,
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
            query_engine: query_engine.clone(),
            table: ScriptsTable::new(catalog_manager, query_engine).await?,
        })
    }

    pub fn create_table_request(&self) -> RegisterSystemTableRequest {
        let request = CreateTableRequest {
            id: SCRIPTS_TABLE_ID,
            catalog_name: DEFAULT_CATALOG_NAME.to_string(),
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            desc: Some("GreptimeDB scripts table for Python".to_string()),
            schema: build_scripts_schema(),
            region_numbers: vec![0],
            // 'schema' and 'name' are primary keys
            primary_key_indices: vec![0, 1],
            create_if_not_exists: true,
            table_options: TableOptions::default(),
            engine: default_engine().to_string(),
        };

        let query_engine = self.query_engine.clone();

        let hook: OpenSystemTableHook = Box::new(move |table: TableRef| {
            let query_engine = query_engine.clone();
            async move { ScriptsTable::recompile_register_udf(table, query_engine.clone()).await }
                .boxed()
        });

        RegisterSystemTableRequest {
            create_table_request: request,
            open_hook: Some(hook),
        }
    }

    /// compile script, and register them to the query engine and UDF registry
    async fn compile(&self, name: &str, script: &str) -> Result<Arc<PyScript>> {
        let script = Arc::new(Self::compile_without_cache(&self.py_engine, name, script).await?);

        {
            let mut compiled = self.compiled.write().unwrap();
            let _ = compiled.insert(name.to_string(), script.clone());
        }
        logging::info!("Compiled and cached script: {}", name);

        script.as_ref().register_udf().await;

        logging::info!("Script register as UDF: {}", name);

        Ok(script)
    }

    /// compile script to PyScript, but not register them to the query engine and UDF registry nor caching in `compiled`
    async fn compile_without_cache(
        py_engine: &PyEngine,
        name: &str,
        script: &str,
    ) -> Result<PyScript> {
        py_engine
            .compile(script, CompileContext::default())
            .await
            .context(CompilePythonSnafu { name })
    }

    pub async fn insert_and_compile(
        &self,
        schema: &str,
        name: &str,
        script: &str,
    ) -> Result<Arc<PyScript>> {
        let compiled_script = self.compile(name, script).await?;
        self.table.insert(schema, name, script).await?;
        Ok(compiled_script)
    }

    pub async fn execute(
        &self,
        schema: &str,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<Output> {
        let script = {
            let s = self.compiled.read().unwrap().get(name).cloned();

            if s.is_some() {
                s
            } else {
                self.try_find_script_and_compile(schema, name).await?
            }
        };

        let script = script.context(ScriptNotFoundSnafu { name })?;

        script
            .execute(params, EvalContext::default())
            .await
            .context(ExecutePythonSnafu { name })
    }

    async fn try_find_script_and_compile(
        &self,
        schema: &str,
        name: &str,
    ) -> Result<Option<Arc<PyScript>>> {
        let script = self.table.find_script_by_name(schema, name).await?;

        Ok(Some(self.compile(name, &script).await?))
    }
}

#[cfg(test)]
mod tests {
    use catalog::memory::MemoryCatalogManager;
    use query::QueryEngineFactory;

    use super::*;

    #[ignore = "script engine is temporary disabled"]
    #[tokio::test]
    async fn test_insert_find_compile_script() {
        common_telemetry::init_default_ut_logging();
        let catalog_manager = MemoryCatalogManager::new();

        let factory = QueryEngineFactory::new(catalog_manager.clone(), None, false);
        let query_engine = factory.query_engine();
        let mgr = ScriptManager::new(catalog_manager.clone(), query_engine)
            .await
            .unwrap();

        let schema = "schema";
        let name = "test";
        mgr.table
            .insert(
                schema,
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
        let script = mgr.try_find_script_and_compile(schema, name).await.unwrap();
        let _ = script.unwrap();

        {
            let cached = mgr.compiled.read().unwrap();
            let _ = cached.get(name).unwrap();
        }
    }
}
