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

use api::v1::ddl_request::Expr;
use api::v1::greptime_request::Request;
use api::v1::{CreateTableExpr, DdlRequest};
use arc_swap::ArcSwap;
use catalog::{CatalogManagerRef, RegisterSystemTableRequest};
use common_catalog::consts::{default_engine, DEFAULT_SCHEMA_NAME};
use common_error::ext::{BoxedError, ErrorExt};
use common_function::function::FunctionRef;
use common_function::function_registry::{FunctionProvider, FUNCTION_REGISTRY};
use common_meta::table_name::TableName;
use common_query::Output;
use common_telemetry::logging;
use query::QueryEngineRef;
use servers::query_handler::grpc::GrpcQueryHandlerRef;
use session::context::{QueryContext, QueryContextRef};
use snafu::{OptionExt, ResultExt};
use table::TableRef;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::error::{
    CatalogSnafu, CompilePythonSnafu, Error, ExecutePythonSnafu, GrpcQuerySnafu, Result,
    ScriptNotFoundSnafu, ScriptsTableNotFoundSnafu,
};
use crate::python::utils::block_on_async;
use crate::python::{PyEngine, PyScript};
use crate::table::{build_scripts_schema, ScriptsTable, ScriptsTableRef, SCRIPTS_TABLE_NAME};

#[derive(Clone)]
struct ScriptWithVersion {
    script: Arc<PyScript>,
    version: u64,
}

pub struct ScriptManager<E: ErrorExt + Send + Sync + 'static> {
    compiled: RwLock<HashMap<String, ScriptWithVersion>>,
    py_engine: PyEngine,
    catalog_manager: CatalogManagerRef,
    grpc_handler: ArcSwap<GrpcQueryHandlerRef<E>>,
    //  Catalog name -> `[ScriptsTable]`
    tables: RwLock<HashMap<String, ScriptsTableRef<E>>>,
    query_engine: QueryEngineRef,
}

impl<E: ErrorExt + Send + Sync + 'static> ScriptManager<E> {
    pub async fn new(
        catalog_manager: CatalogManagerRef,
        grpc_handler: GrpcQueryHandlerRef<E>,
        query_engine: QueryEngineRef,
    ) -> Result<Self> {
        Ok(Self {
            compiled: RwLock::new(HashMap::default()),
            py_engine: PyEngine::new(query_engine.clone()),
            catalog_manager,
            query_engine,
            grpc_handler: ArcSwap::new(Arc::new(grpc_handler)),
            tables: RwLock::new(HashMap::default()),
        })
    }

    pub fn register_as_function_provider(s: Arc<Self>) {
        FUNCTION_REGISTRY.register_provider(s);
    }

    pub fn start(&self, grpc_handler: GrpcQueryHandlerRef<E>) -> Result<()> {
        self.grpc_handler.store(Arc::new(grpc_handler));

        Ok(())
    }

    pub fn create_table_request(&self, catalog: &str) -> RegisterSystemTableRequest {
        let (time_index, primary_keys, column_defs) = build_scripts_schema();

        let create_table_expr = CreateTableExpr {
            catalog_name: catalog.to_string(),
            // TODO(dennis): put the scripts table into `system` schema?
            // We always put the scripts table into `public` schema right now.
            schema_name: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: SCRIPTS_TABLE_NAME.to_string(),
            desc: "GreptimeDB scripts table for Python".to_string(),
            column_defs,
            time_index,
            primary_keys,
            create_if_not_exists: true,
            table_options: Default::default(),
            table_id: None, // Should and will be assigned by Meta.
            engine: default_engine().to_string(),
        };

        RegisterSystemTableRequest {
            create_table_expr,
            open_hook: None,
        }
    }

    /// Create scripts table for the specific catalog if it's not exists.
    /// The function is idempotent and safe to be called more than once for the same catalog
    pub async fn create_table_if_need(&self, catalog: &str) -> Result<()> {
        let scripts_table = self.get_scripts_table(catalog);
        if scripts_table.is_some() {
            return Ok(());
        }
        let RegisterSystemTableRequest {
            create_table_expr: expr,
            open_hook,
        } = self.create_table_request(catalog);

        if let Some(table) = self
            .catalog_manager
            .table(&expr.catalog_name, &expr.schema_name, &expr.table_name)
            .await
            .context(CatalogSnafu)?
        {
            if let Some(open_hook) = open_hook {
                (open_hook)(table.clone()).await.context(CatalogSnafu)?;
            }

            self.insert_scripts_table(catalog, table);

            return Ok(());
        }

        let table_name = TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);

        let _ = self
            .grpc_handler
            .load()
            .do_query(
                Request::Ddl(DdlRequest {
                    expr: Some(Expr::CreateTable(expr)),
                }),
                QueryContext::arc(),
            )
            .await
            .map_err(|e| BoxedError::new(e))
            .context(GrpcQuerySnafu)?;

        let table = self
            .catalog_manager
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| ScriptsTableNotFoundSnafu {})?;

        if let Some(open_hook) = open_hook {
            (open_hook)(table.clone()).await.context(CatalogSnafu)?;
        }

        logging::info!(
            "Created scripts table {}.",
            table.table_info().full_table_name()
        );

        self.insert_scripts_table(catalog, table);

        Ok(())
    }

    /// compile script, and register them to the query engine and UDF registry
    async fn compile(&self, name: &str, script: &str, version: u64) -> Result<ScriptWithVersion> {
        let compiled_script = ScriptWithVersion {
            script: Arc::new(Self::compile_without_cache(&self.py_engine, name, script).await?),
            version,
        };

        {
            let mut compiled = self.compiled.write().unwrap();
            let _ = compiled.insert(name.to_string(), compiled_script.clone());
        }
        logging::info!("Compiled and cached script: {}", name);

        Ok(compiled_script)
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
        version: u64,
    ) -> Result<Arc<PyScript>> {
        let compiled_script = self.compile(name, script, version).await?;
        self.get_scripts_table(catalog)
            .context(ScriptsTableNotFoundSnafu)?
            .insert(schema, name, script, version)
            .await?;

        Ok(compiled_script.script)
    }

    pub async fn execute(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
        params: HashMap<String, String>,
    ) -> Result<Output> {
        let compiled_script = self
            .try_find_script_and_update_cache(catalog, schema, name)
            .await?
            .with_context(|| ScriptNotFoundSnafu { name })?;

        compiled_script
            .script
            .execute(params, EvalContext::default())
            .await
            .context(ExecutePythonSnafu { name })
    }

    pub async fn try_find_script(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<(String, u64)>> {
        let script_and_version = self
            .get_scripts_table(catalog)
            .context(ScriptsTableNotFoundSnafu)?
            .find_script_and_version_by_name(schema, name)
            .await;
        if let Err(Error::ScriptNotFound { .. }) = script_and_version {
            return Ok(None);
        }

        Ok(Some(script_and_version?))
    }

    async fn try_find_script_and_update_cache(
        &self,
        catalog: &str,
        schema: &str,
        name: &str,
    ) -> Result<Option<ScriptWithVersion>> {
        let (script, version) = self
            .try_find_script(catalog, schema, name)
            .await?
            .with_context(|| ScriptNotFoundSnafu { name })?;

        if let Some(s) = self.compiled.read().unwrap().get(name).cloned() {
            if s.version >= version {
                return Ok(Some(s));
            }
        }

        Ok(Some(self.compile(name, &script, version).await?))
    }
}

impl<E: ErrorExt + Send + Sync + 'static> FunctionProvider for ScriptManager<E> {
    fn get_function(&self, name: &str, query_ctx: QueryContextRef) -> Option<FunctionRef> {
        let compiled_script = block_on_async(async {
            self.create_table_if_need(query_ctx.current_catalog())
                .await?;

            self.try_find_script_and_update_cache(
                query_ctx.current_catalog(),
                query_ctx.current_schema(),
                name,
            )
            .await
        });

        compiled_script
            .unwrap_or_else(|e| {
                logging::error!("Thread error: {:?}", e);
                Ok(None)
            })
            .unwrap_or_else(|e| {
                logging::error!("Failed to find script: {:?}", e);
                None
            })
            .map(|s| s.script.udf() as FunctionRef)
    }
}

#[cfg(test)]
mod tests {
    use common_query::OutputData;

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
        let version = 1;

        let mgr = setup_scripts_manager(catalog, schema, name, script, version).await;

        {
            let cached = mgr.compiled.read().unwrap();
            assert!(cached.get(name).is_none());
        }

        mgr.insert_and_compile(catalog, schema, name, script, version)
            .await
            .unwrap();

        {
            let cached = mgr.compiled.read().unwrap();
            assert!(cached.get(name).is_some());
            assert_eq!(cached.get(name).unwrap().version, 1);
        }

        // try to find and compile
        let script = mgr
            .try_find_script_and_update_cache(catalog, schema, name)
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

        match output.data {
            OutputData::RecordBatches(batches) => {
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
