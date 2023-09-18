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

use arc_swap::ArcSwap;
use catalog::{OpenSystemTableHook, RegisterSystemTableRequest};
use common_catalog::consts::{default_engine, DEFAULT_SCHEMA_NAME, SCRIPTS_TABLE_ID};
use common_error::ext::ErrorExt;
use common_query::Output;
use common_telemetry::logging;
use futures::future::FutureExt;
use query::QueryEngineRef;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use snafu::{OptionExt, ResultExt};
use table::requests::{CreateTableRequest, TableOptions};
use table::TableRef;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::error::{
    CompilePythonSnafu, ExecutePythonSnafu, Result, ScriptNotFoundSnafu, ScriptsTableNotFoundSnafu,
};
use crate::python::{PyEngine, PyScript};
use crate::table::{
    build_scripts_schema, get_primary_key_indices, ScriptsTable, ScriptsTableRef,
    SCRIPTS_TABLE_NAME,
};

pub struct ScriptManager<E: ErrorExt + Send + Sync + 'static> {
    compiled: RwLock<HashMap<String, Arc<PyScript>>>,
    py_engine: PyEngine,
    grpc_handler: ArcSwap<GrpcQueryHandlerRef<E>>,
    //  Catalog name -> `[ScriptsTable]`
    tables: RwLock<HashMap<String, ScriptsTableRef<E>>>,
    query_engine: QueryEngineRef,
}

impl<E: ErrorExt + Send + Sync + 'static> ScriptManager<E> {
    pub async fn new(
        grpc_handler: GrpcQueryHandlerRef<E>,
        query_engine: QueryEngineRef,
    ) -> Result<Self> {
        Ok(Self {
            compiled: RwLock::new(HashMap::default()),
            py_engine: PyEngine::new(query_engine.clone()),
            query_engine,
            grpc_handler: ArcSwap::new(Arc::new(grpc_handler)),
            tables: RwLock::new(HashMap::default()),
        })
    }

    pub fn start(&self, grpc_handler: GrpcQueryHandlerRef<E>) -> Result<()> {
        self.grpc_handler.store(Arc::new(grpc_handler));

        Ok(())
    }

    pub fn create_table_request(&self, catalog: &str) -> RegisterSystemTableRequest {
        let request = CreateTableRequest {
            id: SCRIPTS_TABLE_ID,
            catalog_name: catalog.to_string(),
            // TODO(dennis): put the scripts table into `system` schema?
            // We always put the scripts table into `public` schema right now.
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            desc: Some("GreptimeDB scripts table for Python".to_string()),
            schema: build_scripts_schema(),
            region_numbers: vec![0],
            primary_key_indices: get_primary_key_indices(),
            create_if_not_exists: true,
            table_options: TableOptions::default(),
            engine: default_engine().to_string(),
        };

        let query_engine = self.query_engine.clone();

        let hook: OpenSystemTableHook = Box::new(move |table: TableRef| {
            let query_engine = query_engine.clone();
            async move { ScriptsTable::<E>::recompile_register_udf(table, query_engine.clone()).await }
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

    /// Get the scripts table in the catalog
    pub fn get_scripts_table(&self, catalog: &str) -> Option<ScriptsTableRef<E>> {
        self.tables.read().unwrap().get(catalog).cloned()
    }

    /// Insert a scripts table.
    pub fn insert_scripts_table(&self, catalog: &str, table: TableRef) {
        let mut tables = self.tables.write().unwrap();

        if tables.get(catalog).is_some() {
            return;
        }

        tables.insert(
            catalog.to_string(),
            Arc::new(ScriptsTable::new(
                table,
                self.grpc_handler.load().as_ref().clone(),
                self.query_engine.clone(),
            )),
        );
    }

    pub async fn insert_and_compile(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        script: &str,
    ) -> Result<Arc<PyScript>> {
        let compiled_script = self.compile(name, script).await?;
        self.get_scripts_table(catalog)
            .context(ScriptsTableNotFoundSnafu)?
            .insert(schema, name, script)
            .await?;

        Ok(compiled_script)
    }

    pub async fn execute(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<Output> {
        let script = {
            let s = self.compiled.read().unwrap().get(name).cloned();

            if s.is_some() {
                s
            } else {
                self.try_find_script_and_compile(catalog, schema, name)
                    .await?
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
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<Arc<PyScript>>> {
        let script = self
            .get_scripts_table(catalog)
            .context(ScriptsTableNotFoundSnafu)?
            .find_script_by_name(schema, name)
            .await?;

        Ok(Some(self.compile(name, &script).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test::setup_scripts_manager;

    #[tokio::test]
    async fn test_insert_find_compile_script() {
        common_telemetry::init_default_ut_logging();

        let catalog = "greptime";
        let schema = "schema";
        let name = "test";
        let script = r#"
@copr(returns=['n'])
def test() -> vector[str]:
    return 'hello';
"#;

        let mgr = setup_scripts_manager(catalog, schema, name, script).await;

        {
            let cached = mgr.compiled.read().unwrap();
            assert!(cached.get(name).is_none());
        }

        mgr.insert_and_compile(catalog, schema, name, script)
            .await
            .unwrap();

        {
            let cached = mgr.compiled.read().unwrap();
            assert!(cached.get(name).is_some());
        }

        // try to find and compile
        let script = mgr
            .try_find_script_and_compile(catalog, schema, name)
            .await
            .unwrap();
        let _ = script.unwrap();

        {
            let cached = mgr.compiled.read().unwrap();
            let _ = cached.get(name).unwrap();
        }

        // execute script
        let output = mgr
            .execute(catalog, schema, name, HashMap::new())
            .await
            .unwrap();

        match output {
            Output::RecordBatches(batches) => {
                let expected = "\
+-------+
| n     |
+-------+
| hello |
+-------+";
                assert_eq!(expected, batches.pretty_print().unwrap());
            }
            _ => unreachable!(),
        }
    }
}
