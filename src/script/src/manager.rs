//! Scripts manager
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use common_recordbatch::util;
use datatypes::arrow::array::Utf8Array;
use query::{Output, QueryEngineRef};
use snafu::{OptionExt, ResultExt};
use table::engine::TableEngineRef;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::error::{
    CompilePythonSnafu, ExecutePythonSnafu, FindScriptSnafu, Result, ScriptNotFoundSnafu,
};
use crate::python::{PyEngine, PyScript};
use crate::table::ScriptsTable;

pub struct ScriptManager {
    compiled: RwLock<HashMap<String, Arc<PyScript>>>,
    py_engine: PyEngine,
    query_engine: QueryEngineRef,
    table: ScriptsTable,
}

impl ScriptManager {
    pub async fn new(table_engine: TableEngineRef, query_engine: QueryEngineRef) -> Result<Self> {
        Ok(Self {
            compiled: RwLock::new(HashMap::default()),
            py_engine: PyEngine::new(query_engine.clone()),
            query_engine,
            table: ScriptsTable::new(table_engine).await?,
        })
    }

    pub async fn insert_and_compile(&self, name: &str, script: &str) -> Result<Arc<PyScript>> {
        self.table.insert(name, script).await?;
        let script = Arc::new(
            self.py_engine
                .compile(script, CompileContext::default())
                .await
                .context(CompilePythonSnafu)?,
        );

        let mut compiled = self.compiled.write().unwrap();
        compiled.insert(name.to_string(), script.clone());

        Ok(script)
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
            .evaluate(EvalContext::default())
            .await
            .context(ExecutePythonSnafu { name })
    }

    pub async fn delete(_name: &str) -> Result<()> {
        todo!();
    }

    pub async fn update_and_compile(_name: &str) -> Result<()> {
        todo!();
    }

    async fn try_find_script_and_compile(&self, name: &str) -> Result<Option<Arc<PyScript>>> {
        // FIXME(dennis): SQL injection
        let sql = format!("select script from scripts where name='{}'", name);
        let plan = self
            .query_engine
            .sql_to_plan(&sql)
            .context(FindScriptSnafu { name })?;
        let stream = match self
            .query_engine
            .execute(&plan)
            .await
            .context(FindScriptSnafu { name })?
        {
            Output::RecordBatch(stream) => stream,
            _ => unreachable!(),
        };
        let records = util::collect(stream).await.unwrap();

        if records.is_empty() {
            return Ok(None);
        }
        assert!(records.len() == 1);
        assert!(records[0].df_recordbatch.num_columns() == 1);

        let script_column = records[0]
            .df_recordbatch
            .column(0)
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert!(script_column.len() == 1);
        let script = script_column.value(0);

        let script = self.insert_and_compile(name, script).await?;

        Ok(Some(script))
    }
}
