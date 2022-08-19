//! Python script engine
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use async_trait::async_trait;
use common_error::prelude::BoxedError;
use common_recordbatch::{
    error::InnerError as RecordBatchError, error::Result as RecordBatchResult, RecordBatch,
    RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::schema::SchemaRef;
use futures::Stream;
use query::Output;
use query::QueryEngineRef;
use snafu::ensure;
use sql::statements::statement::Statement;

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
                let batch = exec_parsed(&self.copr, &recordbatch.df_recordbatch).map_err(|e| {
                    common_recordbatch::error::Error::from(RecordBatchError::External {
                        source: BoxedError::new(e),
                    })
                })?;

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

    async fn evalute(&self, _ctx: EvalContext) -> Result<Output> {
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
