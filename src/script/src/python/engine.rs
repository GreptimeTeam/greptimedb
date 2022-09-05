//! Python script engine
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_recordbatch::{
    error::ExternalSnafu, error::Result as RecordBatchResult, RecordBatch, RecordBatchStream,
    SendableRecordBatchStream,
};
use common_telemetry::debug;
use datatypes::schema::SchemaRef;
use futures::Stream;
use query::Output;
use query::QueryEngineRef;
use snafu::{ensure, ResultExt};
use sql::statements::statement::Statement;
use tokio::time::{self, Duration};

use crate::engine::{CompileContext, EvalContext, Script, ScriptEngine};
use crate::python::coprocessor::{exec_parsed, parse::parse_copr};
use crate::python::{
    coprocessor::CoprocessorRef,
    error::{self, Result},
};

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
                let batch = exec_parsed(&self.copr, &recordbatch.df_recordbatch)
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

impl PyScript {
    /// repeat a job using a fixed interval
    async fn schedule_job(
        &self,
        dur: Duration,
        _ctx: EvalContext,
        tx: tokio::sync::mpsc::Sender<Result<Output>>,
    ) {
        let mut interval = time::interval(dur);
        loop {
            interval.tick().await;
            if tx.is_closed() {
                debug!(
                    "All receiver to schedule job \"{}\" is closed, ending job now.",
                    self.copr.name
                );
                break;
            }
            let res = tx.send(self.evaluate(EvalContext::default()).await).await;
            if let Err(_err) = res {
                debug!(
                    "All receiver to schedule job \"{}\" is closed, ending job now.",
                    self.copr.name
                );
                break;
            }
        }
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

    async fn evaluate(&self, _ctx: EvalContext) -> Result<Output> {
        if let Some(sql) = &self.copr.deco_args.sql {
            let stmt = self.query_engine.sql_to_statement(sql)?;
            ensure!(
                matches!(stmt, Statement::Query { .. }),
                error::UnsupportedSqlSnafu { sql }
            );
            let plan = self.query_engine.statement_to_plan(stmt)?;
            let res = self.query_engine.execute(&plan).await?;
            let copr = self.copr.clone();
            match res {
                query::Output::RecordBatch(stream) => {
                    Ok(Output::RecordBatch(Box::pin(CoprStream { copr, stream })))
                }
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
        let copr = Arc::new(parse_copr(script)?);

        Ok(PyScript {
            copr,
            query_engine: self.query_engine.clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use catalog::{
        CatalogList, CatalogProvider, SchemaProvider, DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME,
    };
    use common_recordbatch::util;
    use common_telemetry::init_default_ut_logging;
    use datafusion_common::field_util::FieldExt;
    use datafusion_common::field_util::SchemaExt;
    use datatypes::arrow::array::Float64Array;
    use datatypes::arrow::array::Int64Array;
    use query::QueryEngineFactory;
    use table::table::numbers::NumbersTable;

    use super::*;

    #[allow(clippy::print_stdout)]
    #[tokio::test]
    async fn test_schedule() {
        init_default_ut_logging();
        let catalog_list = catalog::memory::new_memory_catalog_list().unwrap();

        let default_schema = Arc::new(MemorySchemaProvider::new());
        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();
        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema);
        catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog);

        let factory = QueryEngineFactory::new(catalog_list);
        let query_engine = factory.query_engine();

        let script_engine = PyEngine::new(query_engine.clone());

        let script = r#"
@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c):
    import greptime as g
    return g.vector([42 + a[0] + b[1] + c[2]])
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();

        let (tx, mut rx) = tokio::sync::mpsc::channel(100);
        tokio::spawn(async move {
            script
                .schedule_job(Duration::from_secs(1), EvalContext::default(), tx)
                .await;
        });
        for _ in 0..10 {
            match rx.recv().await {
                Some(v) => {
                    match v {
                        Ok(out) => {
                            match out {
                                Output::AffectedRows(_) => todo!(),
                                Output::RecordBatch(stream) => {
                                    match util::collect(stream).await {
                                        Ok(rows) => println!("Result: {rows:?}"),
                                        Err(err) => println!("Error: {err:#?}"),
                                    };
                                }
                            }
                            println!("Output something.");
                        }
                        Err(err) => println!("Error: {err}"),
                    };
                }
                None => {
                    debug!("Sender of scheduled job dropped, which is unexpected, something is wrong...");
                    println!("Sender of scheduled job dropped, which is unexpected, something is wrong...");
                }
            }
        }
    }

    #[tokio::test]
    async fn test_compile_evaluate() {
        let catalog_list = catalog::memory::new_memory_catalog_list().unwrap();

        let default_schema = Arc::new(MemorySchemaProvider::new());
        default_schema
            .register_table("numbers".to_string(), Arc::new(NumbersTable::default()))
            .unwrap();
        let default_catalog = Arc::new(MemoryCatalogProvider::new());
        default_catalog.register_schema(DEFAULT_SCHEMA_NAME.to_string(), default_schema);
        catalog_list.register_catalog(DEFAULT_CATALOG_NAME.to_string(), default_catalog);

        let factory = QueryEngineFactory::new(catalog_list);
        let query_engine = factory.query_engine();

        let script_engine = PyEngine::new(query_engine.clone());

        let script = r#"
@copr(args=["a", "b", "c"], returns = ["r"], sql="select number as a,number as b,number as c from numbers limit 100")
def test(a, b, c):
    import greptime as g
    return (a + b) / g.sqrt(c)
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script.evaluate(EvalContext::default()).await.unwrap();
        match output {
            Output::RecordBatch(stream) => {
                let numbers = util::collect(stream).await.unwrap();

                assert_eq!(1, numbers.len());
                let number = &numbers[0];
                assert_eq!(number.df_recordbatch.num_columns(), 1);
                assert_eq!("r", number.schema.arrow_schema().field(0).name());

                let columns = number.df_recordbatch.columns();
                assert_eq!(1, columns.len());
                assert_eq!(100, columns[0].len());
                let rows = columns[0].as_any().downcast_ref::<Float64Array>().unwrap();
                assert!(rows.value(0).is_nan());
                assert_eq!((99f64 + 99f64) / 99f64.sqrt(), rows.value(99))
            }
            _ => unreachable!(),
        }

        // test list comprehension
        let script = r#"
@copr(args=["number"], returns = ["r"], sql="select number from numbers limit 100")
def test(a):
   import greptime as gt
   return gt.vector([x for x in a if x % 2 == 0])
"#;
        let script = script_engine
            .compile(script, CompileContext::default())
            .await
            .unwrap();
        let output = script.evaluate(EvalContext::default()).await.unwrap();
        match output {
            Output::RecordBatch(stream) => {
                let numbers = util::collect(stream).await.unwrap();

                assert_eq!(1, numbers.len());
                let number = &numbers[0];
                assert_eq!(number.df_recordbatch.num_columns(), 1);
                assert_eq!("r", number.schema.arrow_schema().field(0).name());

                let columns = number.df_recordbatch.columns();
                assert_eq!(1, columns.len());
                assert_eq!(50, columns[0].len());
                let rows = columns[0].as_any().downcast_ref::<Int64Array>().unwrap();
                assert_eq!(0, rows.value(0));
                assert_eq!(98, rows.value(49))
            }
            _ => unreachable!(),
        }
    }
}
