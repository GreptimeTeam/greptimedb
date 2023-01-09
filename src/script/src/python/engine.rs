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

//! Python script engine
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_query::Output;
use common_recordbatch::error::{ExternalSnafu, Result as RecordBatchResult};
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures::Stream;
use query::parser::{QueryLanguageParser, QueryStatement};
use query::QueryEngineRef;
use session::context::QueryContext;
use snafu::{ensure, ResultExt};
use sql::statements::statement::Statement;

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::python::coprocessor::{exec_parsed, parse, CoprocessorRef};
use crate::python::error::{self, Result};

const PY_ENGINE: &str = "python";

pub struct PyScript {
    query_engine: QueryEngineRef,
    copr: CoprocessorRef,
}

pub struct CoprStream {
    stream: SendableRecordBatchStream,
    copr: CoprocessorRef,
}

impl RecordBatchStream for CoprStream {
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl Stream for CoprStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(recordbatch))) => {
                let batch = exec_parsed(&self.copr, &recordbatch)
                    .map_err(BoxedError::new)
                    .context(ExternalSnafu)?;

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(other) => Poll::Ready(other),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

#[async_trait]
impl Script for PyScript {
    type Error = error::Error;

    fn engine_name(&self) -> &str {
        PY_ENGINE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn execute(&self, _ctx: EvalContext) -> Result<Output> {
        if let Some(sql) = &self.copr.deco_args.sql {
            let stmt = QueryLanguageParser::parse_sql(sql).unwrap();
            ensure!(
                matches!(stmt, QueryStatement::Sql(Statement::Query { .. })),
                error::UnsupportedSqlSnafu { sql }
            );
            let plan = self
                .query_engine
                .statement_to_plan(stmt, Arc::new(QueryContext::new()))?;
            let res = self.query_engine.execute(&plan).await?;
            let copr = self.copr.clone();
            match res {
                Output::Stream(stream) => Ok(Output::Stream(Box::pin(CoprStream { copr, stream }))),
                _ => unreachable!(),
            }
        } else {
            // TODO(boyan): try to retrieve sql from user request
            error::MissingSqlSnafu {}.fail()
        }
    }
}

pub struct PyEngine {
    query_engine: QueryEngineRef,
}

impl PyEngine {
    pub fn new(query_engine: QueryEngineRef) -> Self {
        Self { query_engine }
    }
}

#[async_trait]
impl ScriptEngine for PyEngine {
    type Error = error::Error;
    type Script = PyScript;

    fn name(&self) -> &str {
        PY_ENGINE
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn compile(&self, script: &str, _ctx: CompileContext) -> Result<PyScript> {
        let copr = Arc::new(parse::parse_and_compile_copr(script)?);

        Ok(PyScript {
            copr,
            query_engine: self.query_engine.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use catalog::local::{MemoryCatalogProvider, MemorySchemaProvider};
    use catalog::{CatalogList, CatalogProvider, SchemaProvider};
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_recordbatch::util;
    use datatypes::prelude::ScalarVector;
    use datatypes::vectors::{Float64Vector, Int64Vector};
    use query::QueryEngineFactory;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[tokio::test]
    async fn test_compile_execute() {
        let catalog_list = catalog::local::new_memory_catalog_list().unwrap();

        let default_schema = Arc::new(MemorySchemaProvider::new());
        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();
        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        default_catalog
            .register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema)
            .unwrap();
        catalog_list
            .register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog)
            .unwrap();

        let factory = QueryEngineFactory::new(catalog_list);
        let query_engine = factory.query_engine();

        let script_engine = PyEngine::new(query_engine.clone());

        // To avoid divide by zero, the script divides `add(a, b)` by `g.sqrt(c + 1)` instead of `g.sqrt(c)`
        let script = r#"
import greptime as g
def add(a, b):
    return a + b;

@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c):
    return add(a, b) / g.sqrt(c + 1)
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script.execute(EvalContext::default()).await.unwrap();
        match output {
            Output::Stream(stream) => {
                let numbers = util::collect(stream).await.unwrap();

                assert_eq!(1, numbers.len());
                let number = &numbers[0];
                assert_eq!(number.num_columns(), 1);
                assert_eq!("r", number.schema.column_schemas()[0].name);

                assert_eq!(1, number.num_columns());
                assert_eq!(100, number.column(0).len());
                let rows = number
                    .column(0)
                    .as_any()
                    .downcast_ref::<Float64Vector>()
                    .unwrap();
                assert_eq!(0f64, rows.get_data(0).unwrap());
                assert_eq!((99f64 + 99f64) / 100f64.sqrt(), rows.get_data(99).unwrap())
            }
            _ => unreachable!(),
        }

        // test list comprehension
        let script = r#"
import greptime as gt

@copr(args=["number"], returns = ["r"], sql="select number from numbers limit 100")
def test(a):
   return gt.vector([x for x in a if x % 2 == 0])
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script.execute(EvalContext::default()).await.unwrap();
        match output {
            Output::Stream(stream) => {
                let numbers = util::collect(stream).await.unwrap();

                assert_eq!(1, numbers.len());
                let number = &numbers[0];
                assert_eq!(number.num_columns(), 1);
                assert_eq!("r", number.schema.column_schemas()[0].name);

                assert_eq!(1, number.num_columns());
                assert_eq!(50, number.column(0).len());
                let rows = number
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Vector>()
                    .unwrap();
                assert_eq!(0, rows.get_data(0).unwrap());
                assert_eq!(98, rows.get_data(49).unwrap())
            }
            _ => unreachable!(),
        }
    }
}
